"""Tests for the ``nexus_transfers.api`` calls over a real SSH connection
(``verify_ssh``, ``delete_ssh``, ``pull_ssh``).

They need passwordless ``ssh localhost``; they are skipped when it is not
available.  The "remote" is a directory under the test's tmp dir.
"""

import asyncio
import os
import shutil

import asyncssh
import pytest

from nexus_transfers import api, check_files_ssh
from nexus_transfers.ssh_ops import check_deletable
from tests.test_check_files_ssh import _ssh_localhost_available

pytestmark = pytest.mark.skipif(
    not _ssh_localhost_available(),
    reason="passwordless ssh to localhost is not available",
)


class Stop(Exception):
    pass


def _stop(*args):
    raise Stop("lease lost")


def _stop_once_started(done, total, files):
    """Abort as soon as some work is done (not while still connecting)."""
    if files:
        raise Stop("lease lost")


def _tree(root, files):
    for rel, data in files.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
    return root


FILES = {".zattrs": b"{}", "data/0.0": b"x" * 100, "data/0.1": b"y" * 50}


def _slow(monkeypatch, cls, method):
    """Make ``cls.method`` sleep first, so a progress tick lands mid-run."""
    orig = getattr(cls, method)

    async def slow(self, *a, **kw):
        await asyncio.sleep(0.05)
        return await orig(self, *a, **kw)
    monkeypatch.setattr(cls, method, slow)


# -- verify_ssh --------------------------------------------------------------

def test_verify_ssh_against_a_local_reference(tmp_path):
    local = _tree(tmp_path / "local", FILES)
    remote = tmp_path / "remote"
    shutil.copytree(local, remote)
    assert api.verify_ssh(f"localhost:{remote}", str(local), checksum=True) == {
        "bytes": 152, "files": 3, "missing": [], "mismatched": [], "extra": []}

    (remote / "data" / "0.1").write_bytes(b"z" * 50)          # same size, other bytes
    (remote / "data" / "0.0").unlink()
    (remote / "stray").write_bytes(b"!")
    by_size = api.verify_ssh(f"localhost:{remote}", local)
    assert by_size == {"bytes": 53, "files": 3, "missing": ["data/0.0"],
                       "mismatched": [], "extra": ["stray"]}
    assert api.verify_ssh(f"localhost:{remote}", local, checksum=True)["mismatched"] == ["data/0.1"]


def test_verify_ssh_against_a_manifest_and_nothing_there(tmp_path):
    remote = _tree(tmp_path / "remote", {"a": b"aa", "b": b"b"})
    got = api.verify_ssh(f"localhost:{remote}", {"a": 2, "b": 5, "c": 1})
    assert got == {"bytes": 3, "files": 2, "missing": ["c"], "mismatched": ["b"], "extra": []}
    calls = []
    assert api.verify_ssh(f"localhost:{tmp_path}/nothing", {"a": 2, "b": 5},
                          progress=lambda *a: calls.append(a)) == {
        "bytes": 0, "files": 0, "missing": ["a", "b"], "mismatched": [], "extra": []}
    assert calls[-1] == (7, 7, 2)


def test_verify_ssh_single_file_target(tmp_path):
    src = tmp_path / "m.ckpt"
    src.write_bytes(b"z" * 10)
    dst = tmp_path / "u.ckpt"                                   # another name
    shutil.copyfile(src, dst)
    clean = {"bytes": 10, "files": 1, "missing": [], "mismatched": [], "extra": []}
    assert api.verify_ssh(f"localhost:{dst}", src, checksum=True) == clean
    assert api.verify_ssh(f"localhost:{dst}", {"whatever.ckpt": 10}) == clean
    assert api.verify_ssh(f"localhost:{dst}", {"x": 11})["mismatched"] == ["x"]


def test_verify_ssh_progress_raising_aborts(tmp_path, monkeypatch):
    local = _tree(tmp_path / "local", {f"f{i}": b"a" for i in range(20)})
    shutil.copytree(local, tmp_path / "remote")
    orig = check_files_ssh.remote_hash
    hashed = []

    async def slow_hash(*a, **kw):
        await asyncio.sleep(0.05)
        hashed.append(a)
        return await orig(*a, **kw)
    monkeypatch.setattr(check_files_ssh, "remote_hash", slow_hash)
    with pytest.raises(Stop):
        api.verify_ssh(f"localhost:{tmp_path / 'remote'}", local, checksum=True,
                       progress=_stop_once_started, progress_interval=0.01,
                       max_concurrent=1)
    assert 0 < len(hashed) < 20


# -- delete_ssh --------------------------------------------------------------

@pytest.mark.parametrize("path, root", [
    ("", None), (".", None), ("..", None), ("relative/a/b", None),
    ("/", None), ("/a", None), ("/a/b/../../c", None),
    ("/data/store", "/data/store"), ("/data/store/", "/data/store"),
    ("/data/other/x", "/data/store"), ("/data/store/../other/x", "/data/store"),
])
def test_delete_ssh_refuses_unsafe_paths(path, root):
    with pytest.raises(ValueError):
        # An unreachable host: the refusal must come before any connection.
        api.delete_ssh(f"no-such-host.invalid:{path}", root=root)


def test_check_deletable_accepts_normal_paths():
    assert check_deletable("/data/store/x.zarr/", "/data/store") == "/data/store/x.zarr"
    assert check_deletable("/a/b/c") == "/a/b/c"


def test_delete_ssh_removes_tree_and_is_idempotent(tmp_path):
    remote = _tree(tmp_path / "store" / "x.zarr", FILES)
    outside = tmp_path / "keep"
    outside.write_bytes(b"k")
    os.symlink(outside, remote / "link")                        # removed, not followed
    (remote / "empty").mkdir()
    calls = []
    got = api.delete_ssh(f"localhost:{remote}", root=str(tmp_path / "store"),
                         progress=lambda *a: calls.append(a))
    assert got["files"] == 4 and got["bytes"] >= 152
    assert not remote.exists() and outside.read_bytes() == b"k"
    assert calls[-1][0] == calls[-1][1] == got["bytes"]
    assert api.delete_ssh(f"localhost:{remote}") == {"bytes": 0, "files": 0}


def test_delete_ssh_single_file(tmp_path):
    f = tmp_path / "u.ckpt"
    f.write_bytes(b"z" * 10)
    assert api.delete_ssh(f"localhost:{f}") == {"bytes": 10, "files": 1}
    assert not f.exists()


def test_delete_ssh_progress_raising_aborts(tmp_path, monkeypatch):
    remote = _tree(tmp_path / "store" / "d", {f"f{i}": b"a" for i in range(20)})
    _slow(monkeypatch, asyncssh.SFTPClient, "remove")
    with pytest.raises(Stop):
        api.delete_ssh(f"localhost:{remote}", progress=_stop_once_started,
                       progress_interval=0.01)
    assert 0 < len(os.listdir(remote)) < 20


# -- pull_ssh ----------------------------------------------------------------

def test_pull_ssh_copies_resumes_and_writes_atomically(tmp_path):
    remote = _tree(tmp_path / "remote", FILES)
    dst = tmp_path / "local" / "x.zarr"
    calls = []
    got = api.pull_ssh(f"localhost:{remote}", str(dst), progress=lambda *a: calls.append(a),
                       progress_interval=0.01)
    assert got == {"bytes": 152, "files": 3, "transferred_bytes": 152, "transferred_files": 3,
                   "skipped_bytes": 0, "skipped_files": 0}
    assert (dst / "data" / "0.0").read_bytes() == b"x" * 100
    assert calls[-1] == (152, 152, 3)
    assert not [p for p in dst.rglob("*.tmp")]

    (dst / "data" / "0.1").write_bytes(b"short")               # a wrong size is re-fetched
    again = api.pull_ssh(f"localhost:{remote}", dst)
    assert again["skipped_files"] == 2 and again["transferred_files"] == 1
    assert (dst / "data" / "0.1").read_bytes() == b"y" * 50


def test_pull_ssh_single_file_and_missing_source(tmp_path):
    src = tmp_path / "m.ckpt"
    src.write_bytes(b"z" * 10)
    dst = tmp_path / "local" / "u.ckpt"
    assert api.pull_ssh(f"localhost:{src}", str(dst))["files"] == 1
    assert dst.read_bytes() == b"z" * 10
    with pytest.raises(FileNotFoundError):
        api.pull_ssh(f"localhost:{tmp_path}/nothing", str(tmp_path / "n"))


def test_pull_ssh_progress_raising_aborts(tmp_path, monkeypatch):
    remote = _tree(tmp_path / "remote", {f"f{i}": b"a" for i in range(20)})
    _slow(monkeypatch, asyncssh.SFTPClient, "get")
    dst = tmp_path / "dst"
    with pytest.raises(Stop):
        api.pull_ssh(f"localhost:{remote}", str(dst), progress=_stop_once_started,
                     progress_interval=0.01, max_concurrent=1)
    got = os.listdir(dst)
    assert 0 < len(got) < 20 and not [n for n in got if n.endswith(".tmp")]


def test_pull_ssh_takes_the_broker_lock(tmp_path, monkeypatch):
    import nexus_transfers.claim as claim
    seen = {}

    async def fake_connect_locked(name, broker_url, *, ssl_verify=True, steal=False):
        seen.update(name=name, broker_url=broker_url, steal=steal)
        return None
    monkeypatch.setattr(claim, "connect_locked", fake_connect_locked)
    remote = _tree(tmp_path / "remote", {"a": b"a"})
    api.pull_ssh(f"localhost:{remote}", str(tmp_path / "dst"),
                 lock="nexus-location-1234", broker_url="ws://broker")
    assert seen == {"name": "nexus-location-1234", "broker_url": "ws://broker", "steal": True}


def test_push_ssh_in_several_processes(tmp_path):
    local = _tree(tmp_path / "local", FILES)
    remote = tmp_path / "remote"
    result = api.push_ssh(str(local), f"localhost:{remote}", processes=2)
    assert result["transferred_files"] == 3 and result["bytes"] == 152
    assert api.verify_ssh(f"localhost:{remote}", str(local), checksum=True)["mismatched"] == []
    again = api.push_ssh(str(local), f"localhost:{remote}", processes=2)
    assert again["skipped_files"] == 3 and again["transferred_files"] == 0
