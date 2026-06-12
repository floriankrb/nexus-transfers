"""Tests for the SSH-mode integrity check (``check-files-ssh``).

These tests need passwordless ``ssh localhost``; they are skipped when it
is not available.
"""

import asyncio
import os
import shutil

import pytest

from nexus_transfers.check_files_ssh import _check_ssh


def _ssh_localhost_available() -> bool:
    import subprocess

    try:
        proc = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no",
             "-o", "ConnectTimeout=3", "localhost", "true"],
            capture_output=True, timeout=10,
        )
        return proc.returncode == 0
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _ssh_localhost_available(),
    reason="passwordless ssh to localhost is not available",
)


def _make_tree(root):
    root.mkdir(exist_ok=True)
    (root / "a.txt").write_text("aaa")
    (root / "sub").mkdir()
    (root / "sub" / "b.txt").write_text("bbb")
    return root


async def _run_check(source, remote_dir, **kwargs):
    return await _check_ssh(
        source=str(source),
        target=f"localhost:{remote_dir}",
        broker_url=None,
        name="checker-ssh",
        site=None,
        ssh_connections=1,
        quiet=True,
        **kwargs,
    )


@pytest.mark.asyncio
async def test_ssh_check_clean(tmp_path):
    local = _make_tree(tmp_path / "local")
    shutil.copytree(local, tmp_path / "remote")

    report = await _run_check(local, tmp_path / "remote")
    assert report.ok
    assert report.checked == 2
    assert report.n_found == 0


@pytest.mark.asyncio
async def test_ssh_check_detects_corruption_and_fixes(tmp_path):
    local = _make_tree(tmp_path / "local")
    remote = tmp_path / "remote"
    shutil.copytree(local, remote)
    (remote / "sub" / "b.txt").write_text("CORRUPTED")
    os.chmod(local / "sub" / "b.txt", 0o600)
    os.chmod(remote / "sub" / "b.txt", 0o600)

    report = await _run_check(local, remote)
    assert not report.ok
    assert [e[0] for e in report.discrepancies["corrupt"]] == ["sub/b.txt"]

    report = await _run_check(local, remote, fix=True)
    assert report.ok
    assert (remote / "sub" / "b.txt").read_text() == "bbb"
    # A fixed file matches the reference fully, mode included.
    assert (os.stat(remote / "sub" / "b.txt").st_mode & 0o7777) == 0o600


@pytest.mark.asyncio
async def test_ssh_check_missing_remote(tmp_path):
    local = _make_tree(tmp_path / "local")
    remote = tmp_path / "remote"
    shutil.copytree(local, remote)
    (remote / "a.txt").unlink()

    report = await _run_check(local, remote)
    assert not report.ok
    assert [e[0] for e in report.discrepancies["missing"]] == ["a.txt"]

    report = await _run_check(local, remote, fix=True)
    assert report.ok
    assert (remote / "a.txt").read_text() == "aaa"


@pytest.mark.asyncio
async def test_ssh_check_extra_remote(tmp_path):
    local = _make_tree(tmp_path / "local")
    remote = tmp_path / "remote"
    shutil.copytree(local, remote)
    (remote / "sub" / "stray.txt").write_text("oops")

    report = await _run_check(local, remote)
    assert not report.ok
    assert [e[0] for e in report.discrepancies["extra"]] == ["sub/stray.txt"]

    report = await _run_check(local, remote, delete_extra=True)
    assert report.ok
    assert not (remote / "sub" / "stray.txt").exists()


@pytest.mark.asyncio
async def test_ssh_check_permissions(tmp_path):
    local = _make_tree(tmp_path / "local")
    remote = tmp_path / "remote"
    shutil.copytree(local, remote)
    os.chmod(local / "a.txt", 0o600)
    os.chmod(remote / "a.txt", 0o644)

    # Without --fix-permissions: drift against the local reference is reported.
    report = await _run_check(local, remote)
    assert not report.ok
    assert len(report.discrepancies["mode"]) == 1

    # With an explicit mode: every remote file is forced to it.
    report = await _run_check(local, remote, fix_permissions=0o600)
    assert report.ok
    for f in ("a.txt", "sub/b.txt"):
        assert (os.stat(remote / f).st_mode & 0o7777) == 0o600
