"""Integration tests – real server + clients over WebSocket."""

import asyncio
import os

import pytest

from nexus_transfers import Client
from nexus_transfers.client import RemoteError


@pytest.mark.asyncio
async def test_rpc_adder(server):
    async with Client("a", url=server) as a, Client("b", url=server) as b:
        result = await b.send("a.adder", 42)
        assert result == 43


@pytest.mark.asyncio
async def test_rpc_echo(server):
    async with Client("a", url=server) as a, Client("b", url=server) as b:
        assert await b.send("a.echo", "hello") == "hello"
        assert await b.send("a.echo", "x", "y") == ["x", "y"]


@pytest.mark.asyncio
async def test_list_clients(server):
    async with Client("alice", url=server) as a, Client("bob", url=server) as b:
        clients = await a.list_clients()
        assert sorted(clients) == ["alice", "bob"]


@pytest.mark.asyncio
async def test_unknown_function(server):
    async with Client("a", url=server) as a, Client("b", url=server) as b:
        with pytest.raises(RemoteError, match="unknown function"):
            await b.send("a.no_such_func")


@pytest.mark.asyncio
async def test_unknown_target(server):
    async with Client("a", url=server) as a:
        with pytest.raises(RemoteError, match="unknown target"):
            await a.send("nobody.adder", 1)


@pytest.mark.asyncio
async def test_duplicate_name(server):
    async with Client("a", url=server) as a:
        with pytest.raises(RuntimeError, match="already taken"):
            async with Client("a", url=server) as a2:
                pass


@pytest.mark.asyncio
async def test_file_transfer(server, tmp_path):
    # Create a test file
    src_file = tmp_path / "test.bin"
    content = bytes(range(256)) * 10
    src_file.write_bytes(content)

    async with (
        Client("sender", url=server, allowed_paths=[str(tmp_path)]) as sender,
        Client("receiver", url=server) as receiver,
    ):
        data = await receiver.send("sender.get_file", str(src_file))
        assert data == content


@pytest.mark.asyncio
async def test_file_transfer_checksum(server, tmp_path):
    src_file = tmp_path / "data.bin"
    src_file.write_bytes(os.urandom(100_000))

    async with (
        Client("s", url=server, allowed_paths=[str(tmp_path)]) as s,
        Client("r", url=server) as r,
    ):
        data = await r.send("s.get_file", str(src_file))
        assert data == src_file.read_bytes()


@pytest.mark.asyncio
async def test_list_dir(server, tmp_path):
    (tmp_path / "file.txt").write_text("hello")
    (tmp_path / "sub").mkdir()

    async with (
        Client("a", url=server, allowed_paths=[str(tmp_path)]) as a,
        Client("b", url=server) as b,
    ):
        entries = await b.send("a.list_dir", str(tmp_path))
        names = {e["name"] for e in entries}
        assert "file.txt" in names
        assert "sub" in names


@pytest.mark.asyncio
async def test_list_dir_path_security(server, tmp_path):
    async with (
        Client("a", url=server, allowed_paths=[str(tmp_path)]) as a,
        Client("b", url=server) as b,
    ):
        with pytest.raises(RemoteError, match="outside"):
            await b.send("a.list_dir", "/etc")


@pytest.mark.asyncio
async def test_get_directory(server, tmp_path):
    # Build a small remote tree
    src = tmp_path / "remote"
    src.mkdir()
    (src / "a.txt").write_text("aaa")
    (src / "sub").mkdir()
    (src / "sub" / "b.txt").write_text("bbb")

    dest = tmp_path / "local"

    async with (
        Client("srv", url=server, allowed_paths=[str(src)]) as srv,
        Client("cli", url=server) as cli,
    ):
        await cli.get_directory("srv", str(src), str(dest))

    assert (dest / "a.txt").read_text() == "aaa"
    assert (dest / "sub" / "b.txt").read_text() == "bbb"


@pytest.mark.asyncio
async def test_get_directory_resume(server, tmp_path):
    src = tmp_path / "remote"
    src.mkdir()
    (src / "done.txt").write_text("done")
    (src / "new.txt").write_text("new")

    dest = tmp_path / "local"
    dest.mkdir()
    # Pre-create done.txt with matching size to simulate completed transfer
    (dest / "done.txt").write_text("done")

    async with (
        Client("srv", url=server, allowed_paths=[str(src)]) as srv,
        Client("cli", url=server) as cli,
    ):
        await cli.get_directory("srv", str(src), str(dest))

    assert (dest / "done.txt").read_text() == "done"
    assert (dest / "new.txt").read_text() == "new"


@pytest.mark.asyncio
async def test_bidirectional_rpc(server):
    async with Client("a", url=server) as a, Client("b", url=server) as b:
        # a calls b, b calls a
        r1 = await a.send("b.adder", 10)
        r2 = await b.send("a.adder", 20)
        assert r1 == 11
        assert r2 == 21
