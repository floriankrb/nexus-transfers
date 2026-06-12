"""Downloaded/uploaded files must always end up with mode 644.

The guarantee must hold whatever the local umask or the remote SSH
server's default happens to be.
"""

import os
import stat
import subprocess

import pytest
from obstore.store import MemoryStore

from nexus_transfers import s3 as _s3
from nexus_transfers.client._io import _write_file
from nexus_transfers.copy_s3 import copy_from_s3, copy_to_s3


@pytest.fixture()
def restrictive_umask():
    """Run the test under umask 077 (the worst case for 644)."""
    old = os.umask(0o077)
    yield
    os.umask(old)


def _mode(path) -> int:
    return stat.S_IMODE(os.stat(path).st_mode)


def test_write_file_mode(tmp_path, restrictive_umask):
    target = tmp_path / "out.bin"
    _write_file(str(target), b"data")
    assert _mode(target) == 0o644


@pytest.mark.asyncio
async def test_s3_download_mode(tmp_path, restrictive_umask, monkeypatch):
    store = MemoryStore()
    monkeypatch.setattr(_s3, "_store_factory", lambda: store)

    src = tmp_path / "src"
    src.mkdir()
    (src / "a.txt").write_text("hello")
    os.chmod(src / "a.txt", 0o600)

    await copy_to_s3(str(src), "s3://bucket/pre", quiet=True)
    dest = tmp_path / "dest"
    await copy_from_s3("s3://bucket/pre", str(dest), quiet=True)
    assert (dest / "a.txt").read_text() == "hello"
    assert _mode(dest / "a.txt") == 0o644


def _ssh_localhost_available() -> bool:
    try:
        proc = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no",
             "-o", "ConnectTimeout=3", "localhost", "true"],
            capture_output=True, timeout=10,
        )
        return proc.returncode == 0
    except Exception:
        return False


@pytest.mark.asyncio
@pytest.mark.skipif(not _ssh_localhost_available(),
                    reason="passwordless ssh to localhost is not available")
async def test_ssh_upload_mode(tmp_path, restrictive_umask):
    from nexus_transfers.ssh import SSHPool, write_file

    src = tmp_path / "src.txt"
    src.write_text("data")
    os.chmod(src, 0o600)
    target = tmp_path / "uploaded.txt"

    async with SSHPool("localhost", 22, None, None, 1, None) as pool:
        await write_file(pool.get_sftp(), str(src), str(target))
    assert _mode(target) == 0o644