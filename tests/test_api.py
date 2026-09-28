"""Tests for nexus_transfers.api (the importable transfer API).

SSH is replaced by a fake pool whose SFTP client writes to a local directory,
so the real copy/resume/progress machinery runs without a server.
"""

import os
import shutil

import pytest

from nexus_transfers import api, copy_ssh


class _Attrs:
    def __init__(self, path):
        st = os.stat(path)
        self.size = st.st_size
        self.permissions = st.st_mode


class _Entry:
    def __init__(self, dirpath, name):
        self.filename = name
        self.attrs = _Attrs(os.path.join(dirpath, name))


class _FakeSFTP:
    """The subset of asyncssh's SFTP client the copy uses, on the local disk."""

    def __init__(self, puts):
        self.puts = puts

    async def stat(self, path):
        import asyncssh
        if not os.path.exists(path):
            raise asyncssh.SFTPNoSuchFile(path)
        return _Attrs(path)

    async def readdir(self, path):
        return [_Entry(path, n) for n in os.listdir(path)]


class _FakePool:
    def __init__(self, *a, **kw):
        self.sftp = _FakeSFTP(_FakePool.puts)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None

    def get_sftp(self):
        return self.sftp


async def _fake_write_file(sftp, local_path, remote_path):
    os.makedirs(os.path.dirname(remote_path), exist_ok=True)
    shutil.copyfile(local_path, remote_path)
    sftp.puts.append(remote_path)


@pytest.fixture
def fake_ssh(monkeypatch):
    _FakePool.puts = []
    monkeypatch.setattr(copy_ssh, "SSHPool", _FakePool)
    monkeypatch.setattr(copy_ssh, "write_file", _fake_write_file)

    async def _stat_remote(sftp, path):
        return os.path.getsize(path) if os.path.exists(path) else None
    monkeypatch.setattr(copy_ssh, "stat_remote", _stat_remote)
    import nexus_transfers.ssh as ssh_mod
    monkeypatch.setattr(ssh_mod, "SSHPool", _FakePool)
    return _FakePool.puts


def _tree(root, files):
    for rel, data in files.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
    return root


def test_push_ssh_copies_reports_progress_and_resumes(tmp_path, fake_ssh):
    src = _tree(tmp_path / "src.zarr", {".zattrs": b"{}", "data/0.0": b"x" * 100, "data/0.1": b"y" * 50})
    dst = tmp_path / "remote" / "x.zarr"
    calls = []
    result = api.push_ssh(str(src), f"host:{dst}", progress=lambda *a: calls.append(a),
                          progress_interval=0.01)
    assert result["bytes"] == 152 and result["files"] == 3
    assert result["transferred_files"] == 3 and result["skipped_files"] == 0
    assert (dst / "data" / "0.0").read_bytes() == b"x" * 100
    assert calls[-1] == (152, 152, 3)                 # the final call is complete
    assert api.stat_ssh(f"host:{dst}") == {"bytes": 152, "files": 3}

    fake_ssh.clear()
    again = api.push_ssh(str(src), f"host:{dst}", progress=lambda *a: None)
    assert fake_ssh == [] and again["skipped_files"] == 3   # resumed: nothing re-sent


def test_push_ssh_single_file_target_is_the_file(tmp_path, fake_ssh):
    src = tmp_path / "m.ckpt"
    src.write_bytes(b"z" * 10)
    dst = tmp_path / "remote" / "u.ckpt"
    result = api.push_ssh(str(src), f"host:{dst}")
    assert dst.read_bytes() == b"z" * 10 and result["files"] == 1
    assert api.stat_ssh(f"host:{dst}") == {"bytes": 10, "files": 1}
    assert api.stat_ssh(f"host:{tmp_path}/nothing") is None


def test_progress_callback_raising_aborts_the_copy(tmp_path, fake_ssh, monkeypatch):
    src = _tree(tmp_path / "src", {f"f{i}": b"a" for i in range(20)})

    async def _slow_write(sftp, local_path, remote_path):
        import asyncio
        await asyncio.sleep(0.05)
        await _fake_write_file(sftp, local_path, remote_path)
    monkeypatch.setattr(copy_ssh, "write_file", _slow_write)

    class Stop(Exception):
        pass

    def progress(done, total, files):
        raise Stop("lease lost")

    with pytest.raises(Stop):
        api.push_ssh(str(src), f"host:{tmp_path / 'dst'}", progress=progress,
                     progress_interval=0.01, max_concurrent=1)
    assert len(fake_ssh) < 20                         # stopped before the end
