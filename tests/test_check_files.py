"""Tests for the relay-mode integrity check (``check-files``)."""

import argparse
import os
import time

import pytest

from nexus_transfers import Client
from nexus_transfers.check_files import (
    CheckFailedError,
    _parse_age,
    check_files,
    is_failed_transfer_leftover,
)
from nexus_transfers.client import RemoteError


def _make_tree(root):
    """Create a small reference tree and return it."""
    root.mkdir(exist_ok=True)
    (root / "a.txt").write_text("aaa")
    (root / "sub").mkdir()
    (root / "sub" / "b.txt").write_text("bbb")
    (root / "sub" / "deep").mkdir()
    (root / "sub" / "deep" / "c.bin").write_bytes(bytes(range(200)))
    return root


def _clone_tree(src, dst):
    """Copy the reference tree to *dst* (content and modes)."""
    import shutil

    shutil.copytree(src, dst)


async def _run_check(broker, remote_root, local_root, **kwargs):
    kwargs.setdefault("use_s3", False)
    async with Client("ref", url=broker, allowed_paths=[str(remote_root)]):
        return await check_files(
            name="checker",
            broker_url=broker,
            remote_client="ref",
            source=str(remote_root),
            target=str(local_root),
            quiet=True,
            **kwargs,
        )


@pytest.mark.asyncio
async def test_hash_file_rpc(broker, tmp_path):
    f = tmp_path / "x.bin"
    f.write_bytes(b"hello world")
    async with (
        Client("a", url=broker, allowed_paths=[str(tmp_path)]),
        Client("b", url=broker) as b,
    ):
        info = await b.send("a.hash_file", str(f))
        assert info["hash"] == "5eb63bbbe01eeed093cb22bb8f5acdc3"  # md5
        assert info["algo"] == "md5"
        assert info["size"] == 11
        assert info["mode"] == (os.stat(f).st_mode & 0o7777)


@pytest.mark.asyncio
async def test_hash_file_path_security(broker, tmp_path):
    async with (
        Client("a", url=broker, allowed_paths=[str(tmp_path)]),
        Client("b", url=broker) as b,
    ):
        with pytest.raises(RemoteError, match="outside"):
            await b.send("a.hash_file", "/etc/passwd")


@pytest.mark.asyncio
async def test_check_clean(broker, tmp_path):
    remote = _make_tree(tmp_path / "remote")
    _clone_tree(remote, tmp_path / "local")

    report = await _run_check(broker, remote, tmp_path / "local")
    assert report.ok
    assert report.checked == 3
    assert report.n_found == 0


@pytest.mark.asyncio
async def test_check_detects_corruption(broker, tmp_path):
    remote = _make_tree(tmp_path / "remote")
    local = tmp_path / "local"
    _clone_tree(remote, local)
    (local / "sub" / "b.txt").write_text("CORRUPTED")

    report = await _run_check(broker, remote, local)
    assert not report.ok
    assert len(report.discrepancies["corrupt"]) == 1
    rel, _detail, fix = report.discrepancies["corrupt"][0]
    assert rel == "sub/b.txt"
    assert fix is None


@pytest.mark.asyncio
async def test_check_fix_redownloads(broker, tmp_path):
    remote = _make_tree(tmp_path / "remote")
    local = tmp_path / "local"
    _clone_tree(remote, local)
    (local / "sub" / "b.txt").write_text("CORRUPTED")
    (local / "a.txt").unlink()  # also a missing file
    os.chmod(remote / "a.txt", 0o600)  # reference mode != umask default

    report = await _run_check(broker, remote, local, fix=True)
    assert report.ok
    assert len(report.discrepancies["corrupt"]) == 1
    assert len(report.discrepancies["missing"]) == 1
    assert (local / "sub" / "b.txt").read_text() == "bbb"
    assert (local / "a.txt").read_text() == "aaa"
    # A fixed file matches the reference fully, mode included.
    assert (os.stat(local / "a.txt").st_mode & 0o7777) == 0o600


@pytest.mark.asyncio
async def test_check_extra_files(broker, tmp_path):
    remote = _make_tree(tmp_path / "remote")
    local = tmp_path / "local"
    _clone_tree(remote, local)
    (local / "sub" / "stray.txt").write_text("oops")

    report = await _run_check(broker, remote, local)
    assert not report.ok
    extras = report.discrepancies["extra"]
    assert [e[0] for e in extras] == ["sub/stray.txt"]


def test_is_failed_transfer_leftover():
    ref = {"a.txt", "sub/b.txt", "data/0.1.2"}
    # write_file tmp name, with and without the .tmp suffix
    assert is_failed_transfer_leftover("a.txt.3fa9c2d1.tmp", ref)
    assert is_failed_transfer_leftover("sub/b.txt.deadbe", ref)
    assert is_failed_transfer_leftover("data/0.1.2.abc123", ref)
    # base not on the reference
    assert not is_failed_transfer_leftover("stray.txt.3fa9c2d1.tmp", ref)
    # no hex suffix
    assert not is_failed_transfer_leftover("stray.txt", ref)
    assert not is_failed_transfer_leftover("a.txt.backup", ref)
    # hex too short / not hex
    assert not is_failed_transfer_leftover("a.txt.3fa9", ref)
    assert not is_failed_transfer_leftover("a.txt.zzzzzz", ref)


@pytest.mark.asyncio
async def test_check_delete_extra_only_leftovers(broker, tmp_path):
    remote = _make_tree(tmp_path / "remote")
    local = tmp_path / "local"
    _clone_tree(remote, local)
    # Failed-transfer debris: deletable.
    (local / "sub" / "b.txt.3fa9c2d1.tmp").write_text("partial")
    # Unexplained extra: must never be deleted.
    (local / "stray.txt").write_text("oops")

    report = await _run_check(broker, remote, local, delete_extra=True)
    assert not report.ok  # the stray remains an unfixed discrepancy
    assert not (local / "sub" / "b.txt.3fa9c2d1.tmp").exists()
    assert (local / "stray.txt").exists()
    extras = {rel: fix for rel, _d, fix in report.discrepancies["extra"]}
    assert extras == {"sub/b.txt.3fa9c2d1.tmp": "deleted", "stray.txt": None}


@pytest.mark.asyncio
async def test_check_refuses_empty_reference(broker, tmp_path):
    (tmp_path / "remote").mkdir()  # reference exists but holds no files
    local = tmp_path / "local"
    local.mkdir()
    (local / "precious.txt").write_text("data")

    with pytest.raises(CheckFailedError, match="contains no files"):
        await _run_check(broker, tmp_path / "remote", local, delete_extra=True)
    assert (local / "precious.txt").exists()


@pytest.mark.asyncio
async def test_check_permissions(broker, tmp_path):
    remote = _make_tree(tmp_path / "remote")
    local = tmp_path / "local"
    _clone_tree(remote, local)
    os.chmod(remote / "a.txt", 0o600)
    os.chmod(local / "a.txt", 0o644)

    # Without --fix-permissions: drift against the reference is reported.
    report = await _run_check(broker, remote, local)
    assert not report.ok
    assert len(report.discrepancies["mode"]) == 1

    # With an explicit mode: every local file is forced to it.
    report = await _run_check(broker, remote, local, fix_permissions=0o600)
    assert report.ok
    for f in ("a.txt", "sub/b.txt", "sub/deep/c.bin"):
        assert (os.stat(local / f).st_mode & 0o7777) == 0o600


def test_parse_age():
    assert _parse_age("30d") == 30 * 86400
    assert _parse_age("1h") == 3600
    assert _parse_age("45m") == 2700
    assert _parse_age("2w") == 2 * 604800
    assert _parse_age("4") == 4
    assert _parse_age("4s") == 4
    with pytest.raises(argparse.ArgumentTypeError):
        _parse_age("yesterday")


@pytest.mark.asyncio
async def test_check_max_age_skips_old_files(broker, tmp_path):
    remote = _make_tree(tmp_path / "remote")
    local = tmp_path / "local"
    _clone_tree(remote, local)
    # Corrupt two local files; make one of them look 10 days old.
    (local / "a.txt").write_text("BAD")
    (local / "sub" / "b.txt").write_text("BAD")
    old = time.time() - 10 * 86400
    os.utime(local / "sub" / "b.txt", (old, old))

    report = await _run_check(broker, remote, local, max_age=86400)
    assert report.skipped == 1
    assert report.checked == 2
    # Only the recent corruption is detected; the old file was skipped.
    assert [e[0] for e in report.discrepancies["corrupt"]] == ["a.txt"]

    # Without max_age, both corruptions are found.
    report = await _run_check(broker, remote, local)
    assert report.skipped == 0
    assert len(report.discrepancies["corrupt"]) == 2


@pytest.mark.asyncio
async def test_check_old_server_without_hash_file(broker, tmp_path):
    """A pre-hash_file server must produce a clear error, not a traceback."""
    remote = _make_tree(tmp_path / "remote")
    _clone_tree(remote, tmp_path / "local")

    async with Client("ref", url=broker,
                      allowed_paths=[str(remote)]) as ref:
        del ref.dispatch["hash_file"]  # simulate an old server
        with pytest.raises(CheckFailedError, match="older nexus-transfers"):
            await check_files(
                name="checker",
                broker_url=broker,
                remote_client="ref",
                source=str(remote),
                target=str(tmp_path / "local"),
                quiet=True,
                use_s3=False,
            )


@pytest.mark.asyncio
async def test_check_monitor_events(broker, tmp_path):
    remote = _make_tree(tmp_path / "remote")
    local = tmp_path / "local"
    _clone_tree(remote, local)
    (local / "a.txt").write_text("BAD")

    events = []

    async def _collect(message, status=None, **kw):
        events.append((message, status))

    report = await _run_check(broker, remote, local, on_monitor=_collect)
    assert not report.ok
    # Final summary must always be emitted, with error status.
    final = [e for e in events if "finished" in e[0]]
    assert len(final) == 1
    assert final[0][1] == "error"
    assert "1 corrupt" in final[0][0]

    # Clean run reports ok.
    events.clear()
    (local / "a.txt").write_text("aaa")
    report = await _run_check(broker, remote, local, on_monitor=_collect)
    assert report.ok
    final = [e for e in events if "finished" in e[0]]
    assert final[0][1] == "ok"
    assert "no discrepancy" in final[0][0]
