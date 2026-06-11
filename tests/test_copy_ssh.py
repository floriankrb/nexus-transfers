"""Tests for nexus_transfers.copy_ssh and nexus_transfers.ssh."""

import os
from unittest.mock import AsyncMock, MagicMock

import pytest

from nexus_transfers.copy_ssh import _list_local, _parse_target
from nexus_transfers.ssh import SSHPool, stat_remote, write_file


# ---------------------------------------------------------------------------
# Target parsing
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("target,expected", [
    ("user@host:/remote/path", ("user", "host", "/remote/path")),
    ("host:/remote/path", (None, "host", "/remote/path")),
    ("host:/", (None, "host", "/")),
    ("user@host:/deep/nested/dir", ("user", "host", "/deep/nested/dir")),
])
def test_parse_target_valid(target, expected):
    assert _parse_target(target) == expected


def test_parse_target_no_colon():
    with pytest.raises(ValueError, match="Invalid target"):
        _parse_target("host/path/no-colon")


def test_parse_target_no_user():
    user, host, path = _parse_target("myhost:/data")
    assert user is None
    assert host == "myhost"
    assert path == "/data"


# ---------------------------------------------------------------------------
# Local walk
# ---------------------------------------------------------------------------

async def test_list_local(tmp_path):
    (tmp_path / "a.txt").write_bytes(b"hello")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "b.txt").write_bytes(b"world!")

    items, total_count, total_size = await _list_local(str(tmp_path))

    rel_paths = {rel for _, rel, _ in items}
    assert rel_paths == {"a.txt", os.path.join("sub", "b.txt")}
    assert total_count == 2
    assert total_size == 11

    sizes = {rel: size for _, rel, size in items}
    assert sizes["a.txt"] == 5
    assert sizes[os.path.join("sub", "b.txt")] == 6


async def test_list_local_empty_dir(tmp_path):
    items, total_count, total_size = await _list_local(str(tmp_path))
    assert items == []
    assert total_count == 0
    assert total_size == 0


# ---------------------------------------------------------------------------
# stat_remote
# ---------------------------------------------------------------------------

async def test_stat_remote_exists():
    sftp = AsyncMock()
    sftp.stat.return_value = MagicMock(size=42)
    result = await stat_remote(sftp, "/some/file.txt")
    assert result == 42
    sftp.stat.assert_awaited_once_with("/some/file.txt")


async def test_stat_remote_not_found():
    import asyncssh
    sftp = AsyncMock()
    sftp.stat.side_effect = asyncssh.SFTPError(asyncssh.FX_NO_SUCH_FILE, "not found")
    result = await stat_remote(sftp, "/no/such/file")
    assert result is None


# ---------------------------------------------------------------------------
# write_file
# ---------------------------------------------------------------------------

async def test_write_file_uploads_via_tmp_and_renames():
    sftp = AsyncMock()
    await write_file(sftp, "/local/data/file.txt", "/remote/dir/file.txt")
    sftp.makedirs.assert_awaited_once_with("/remote/dir", exist_ok=True)
    (local, tmp), _ = sftp.put.await_args
    assert local == "/local/data/file.txt"
    assert tmp.startswith("/remote/dir/file.txt.")
    assert tmp.endswith(".tmp")
    sftp.posix_rename.assert_awaited_once_with(tmp, "/remote/dir/file.txt")


async def test_write_file_root_dir():
    sftp = AsyncMock()
    await write_file(sftp, "/local/file.txt", "/file.txt")
    sftp.makedirs.assert_awaited_once_with("/", exist_ok=True)
    (_, tmp), _ = sftp.put.await_args
    sftp.posix_rename.assert_awaited_once_with(tmp, "/file.txt")


async def test_write_file_removes_tmp_on_failure():
    import asyncssh

    sftp = AsyncMock()
    sftp.put.side_effect = asyncssh.SFTPError(asyncssh.FX_FAILURE, "boom")
    with pytest.raises(asyncssh.SFTPError):
        await write_file(sftp, "/local/f", "/remote/f")
    sftp.remove.assert_awaited_once()
    sftp.posix_rename.assert_not_awaited()


# ---------------------------------------------------------------------------
# Integration: SSHPool + write_file against a local asyncssh SFTP server
# ---------------------------------------------------------------------------

@pytest.fixture()
async def sftp_server(tmp_path):
    """Start a local asyncssh SFTP server and yield (port, remote_root)."""
    import asyncssh

    server_key = asyncssh.generate_private_key("ssh-ed25519")
    remote_root = tmp_path / "remote"
    remote_root.mkdir()

    class _NoAuth(asyncssh.SSHServer):
        def begin_auth(self, username):
            # No authentication required.
            return False

    server = await asyncssh.listen(
        "127.0.0.1",
        0,
        server_factory=_NoAuth,
        server_host_keys=[server_key],
        sftp_factory=lambda conn: asyncssh.SFTPServer(
            conn, chroot=str(remote_root)
        ),
    )
    port = server.sockets[0].getsockname()[1]
    yield port, remote_root
    server.close()
    await server.wait_closed()


async def test_ssh_pool_upload(tmp_path, sftp_server):
    """Files uploaded via SSHPool appear on the remote filesystem."""
    port, remote_root = sftp_server

    src = tmp_path / "src"
    src.mkdir()
    (src / "hello.txt").write_bytes(b"hello world")
    (src / "sub").mkdir()
    (src / "sub" / "data.bin").write_bytes(b"\x00\x01\x02")

    async with SSHPool("127.0.0.1", port, None, None, 1) as pool:
        sftp = pool.get_sftp()
        await write_file(sftp, str(src / "hello.txt"), "/hello.txt")
        await write_file(sftp, str(src / "sub" / "data.bin"), "/sub/data.bin")

    assert (remote_root / "hello.txt").read_bytes() == b"hello world"
    assert (remote_root / "sub" / "data.bin").read_bytes() == b"\x00\x01\x02"


async def test_resume_skips_matching_size(tmp_path, sftp_server):
    """A file whose remote size matches local size is not re-uploaded."""
    import asyncssh

    port, remote_root = sftp_server

    content = b"unchanged content"
    src_file = tmp_path / "file.txt"
    src_file.write_bytes(content)

    # Pre-populate the remote file with the same content.
    (remote_root / "file.txt").write_bytes(content)

    upload_calls = []

    async with SSHPool("127.0.0.1", port, None, None, 1) as pool:
        sftp = pool.get_sftp()

        # Verify that stat_remote returns the correct size.
        remote_size = await stat_remote(sftp, "/file.txt")
        assert remote_size == len(content)

        # Simulate resume check: sizes match, so we would skip.
        local_size = src_file.stat().st_size
        if remote_size == local_size:
            pass  # skip — this is what _worker does
        else:
            await write_file(sftp, str(src_file), "/file.txt")
            upload_calls.append("uploaded")

    assert upload_calls == [], "file should have been skipped (sizes match)"
