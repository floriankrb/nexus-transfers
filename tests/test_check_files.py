"""Tests for the relay-mode integrity check (``check-files``)."""

import os

import pytest

from nexus_transfers import Client
from nexus_transfers.check_files import check_files
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


@pytest.mark.asyncio
async def test_check_delete_extra(broker, tmp_path):
    remote = _make_tree(tmp_path / "remote")
    local = tmp_path / "local"
    _clone_tree(remote, local)
    (local / "stray.txt").write_text("oops")

    report = await _run_check(broker, remote, local, delete_extra=True)
    assert report.ok
    assert not (local / "stray.txt").exists()
    assert report.discrepancies["extra"][0][2] == "deleted"


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
