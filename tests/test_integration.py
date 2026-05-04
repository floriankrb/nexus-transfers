"""Integration tests – real server + clients over WebSocket."""

import asyncio
import os

import pytest

from nexus_transfers import Client
from nexus_transfers.client import NameTakenError, PeerNotFoundError, RemoteError
from nexus_transfers.dispatch import DISPATCH


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
        with pytest.raises(PeerNotFoundError, match="unknown target"):
            await a.send("nobody.adder", 1)


@pytest.mark.asyncio
async def test_duplicate_name(server):
    async with Client("a", url=server) as a:
        with pytest.raises(NameTakenError, match="already taken"):
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
        data = await receiver.send("sender.get_file", str(src_file),
                                   use_s3=False)
        assert data == content


@pytest.mark.asyncio
async def test_file_transfer_checksum(server, tmp_path):
    src_file = tmp_path / "data.bin"
    src_file.write_bytes(os.urandom(100_000))

    async with (
        Client("s", url=server, allowed_paths=[str(tmp_path)]) as s,
        Client("r", url=server) as r,
    ):
        data = await r.send("s.get_file", str(src_file), use_s3=False)
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
        await cli.get_directory("srv", str(src), str(dest), use_s3=False)

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
        await cli.get_directory("srv", str(src), str(dest), use_s3=False)

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


@pytest.mark.asyncio
async def test_call_timeout(server):
    """send() should raise TimeoutError when call_timeout is exceeded."""
    async with (
        Client("a", url=server) as a,
        Client("b", url=server, call_timeout=0.1) as b,
    ):
        # Target a function that doesn't exist on the server —
        # we just need the future to never resolve.
        # Instead, we'll use a peer that exists but never replies.
        # Simplest: call a non-existent peer so the error comes via
        # PeerNotFoundError which is quick. Let's instead use a real
        # scenario: monkey-patch a slow function.
        import time

        def slow(*a, **kw):
            time.sleep(5)

        a.dispatch["slow"] = slow
        with pytest.raises(asyncio.TimeoutError):
            await b.send("a.slow")


@pytest.mark.asyncio
async def test_peer_retry_succeeds(server):
    """send() retries when the peer is not found and eventually succeeds."""
    async with Client("caller", url=server, peer_retries=5, peer_delay=0.1) as caller:
        # Launch the peer after a short delay
        async def _delayed_peer():
            await asyncio.sleep(0.25)
            async with Client("late", url=server) as late:
                await asyncio.sleep(2)

        task = asyncio.create_task(_delayed_peer())
        result = await caller.send("late.adder", 99)
        assert result == 100
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_peer_retry_exhausted(server):
    """send() raises PeerNotFoundError when retries are exhausted."""
    async with Client("a", url=server, peer_retries=2, peer_delay=0.05) as a:
        with pytest.raises(PeerNotFoundError):
            await a.send("ghost.adder", 1)


@pytest.mark.asyncio
async def test_atomic_file_write(server, tmp_path):
    """Files should be written atomically — no partial files on disk."""
    src_file = tmp_path / "big.bin"
    content = os.urandom(50_000)
    src_file.write_bytes(content)

    dest = tmp_path / "output"
    dest.mkdir()
    dest_file = dest / "big.bin"

    async with (
        Client("s", url=server, allowed_paths=[str(tmp_path)]) as s,
        Client("r", url=server) as r,
    ):
        data = await r.send("s.get_file", str(src_file), use_s3=False)
        from nexus_transfers.client import _write_file
        _write_file(str(dest_file), data)

    assert dest_file.read_bytes() == content


@pytest.mark.asyncio
async def test_get_directory_resume_skips_complete(server, tmp_path):
    """get_directory skips files whose local size matches the remote size."""
    src = tmp_path / "remote"
    src.mkdir()
    content_a = b"already_done"
    content_b = b"needs_transfer"
    (src / "a.txt").write_bytes(content_a)
    (src / "b.txt").write_bytes(content_b)

    dest = tmp_path / "local"
    dest.mkdir()
    # Pre-create a.txt with matching size
    (dest / "a.txt").write_bytes(content_a)

    async with (
        Client("srv", url=server, allowed_paths=[str(src)]) as srv,
        Client("cli", url=server) as cli,
    ):
        await cli.get_directory("srv", str(src), str(dest), use_s3=False)

    assert (dest / "a.txt").read_bytes() == content_a
    assert (dest / "b.txt").read_bytes() == content_b


@pytest.mark.asyncio
async def test_monitor_receives_messages(server):
    """The monitor peer receives log messages from other clients."""
    received = []

    def log(message, status=None):
        received.append((message, status))
        return True

    dispatch = dict(DISPATCH)
    dispatch["log"] = log

    async with (
        Client("monitor", url=server, dispatch=dispatch) as mon,
        Client("worker", url=server) as worker,
    ):
        await worker.monitor("hello world", status="ok")
        await worker.monitor("just a message")
        # Give the monitor a moment to process
        await asyncio.sleep(0.1)

    # Filter out lifecycle events emitted by connect()/close() so the
    # assertion focuses on the explicit monitor() calls above.
    payload = [(msg, status) for msg, status in received
               if "connected to" not in msg and "disconnecting" not in msg]
    assert payload == [("hello world", "ok"), ("just a message", None)]
    # And confirm the lifecycle events were observed too.
    assert any("worker: connected to" in msg for msg, _ in received)
    assert any("worker: disconnecting from" in msg for msg, _ in received)


@pytest.mark.asyncio
async def test_monitor_no_peer_does_not_block(server):
    """monitor() does not block or raise when no monitor peer is connected."""
    async with Client("lonely", url=server) as client:
        # Should return quickly without error
        await client.monitor("nobody is listening", status="progress")


@pytest.mark.asyncio
async def test_transfer_retries_on_peer_kill(server, tmp_path):
    """Transfers retry and maintain parallelism when the peer dies and restarts."""
    src = tmp_path / "remote"
    src.mkdir()
    for i in range(6):
        (src / f"f{i}.txt").write_bytes(f"content-{i}".encode())

    dest = tmp_path / "local"

    async with Client("cli", url=server, call_timeout=1.0,
                       peer_retries=-1, peer_delay=0.1) as cli:
        # Start the provider, let it register
        provider = Client("prov", url=server, allowed_paths=[str(src)])
        await provider.connect()

        # Start the directory copy in background
        copy_task = asyncio.create_task(
            cli.get_directory("prov", str(src), str(dest),
                              max_concurrent=4, use_s3=False)
        )

        # Give it a moment then kill the provider
        await asyncio.sleep(0.2)
        await provider.close()

        # Restart the provider after a short pause
        await asyncio.sleep(0.3)
        provider2 = Client("prov", url=server, allowed_paths=[str(src)])
        await provider2.connect()

        # Wait for copy to finish
        await asyncio.wait_for(copy_task, timeout=30)
        await provider2.close()

    for i in range(6):
        assert (dest / f"f{i}.txt").read_bytes() == f"content-{i}".encode()
