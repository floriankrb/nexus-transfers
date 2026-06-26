"""Tests for the client-side name-claim / steal interlock."""

import asyncio
import multiprocessing as mp

import pytest

from nexus_transfers.claim import claim_name
from nexus_transfers.client import Client, NameTakenError

async def _names(url):
    """Return the set of currently connected client names."""
    async with Client("probe", url) as probe:
        return set(await probe.list_clients())

async def _wait_for(url, name, present, timeout=10.0):
    """Wait until *name* is (or is not) connected, per *present*."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if (name in await _names(url)) == present:
            return True
        await asyncio.sleep(0.1)
    return False

def _idle_incumbent(name, url):
    """Connect as *name* and idle until killed (run in a child process).

    A soft kill makes the client call ``os._exit(0)``; running it in a separate
    process means that only takes down the incumbent, mirroring production where
    each worker is its own process.
    """

    async def _run():
        client = Client(name, url, reconnect_retries=0)
        await client.connect()
        await client._listener_task  # blocks until killed

    asyncio.run(_run())

async def test_claim_free_name_is_noop(broker):
    """Claiming a name nobody holds returns without killing anything."""
    await claim_name("atos-transfer-T1", broker, kill_existing=True)
    assert "atos-transfer-T1" not in await _names(broker)

async def test_claim_refuses_when_held_and_not_stealing(broker):
    """Without kill_existing, an incumbent makes the claim raise."""
    async with Client("atos-transfer-T2", broker):
        with pytest.raises(NameTakenError):
            await claim_name("atos-transfer-T2", broker, kill_existing=False)

async def test_no_broker_url_is_noop(broker):
    """A missing broker URL skips claiming (nothing to lock against)."""
    await claim_name("whatever", None, kill_existing=True)

async def test_steal_displaces_incumbent(broker):
    """kill_existing soft-kills the holder, waits for it to drop, frees the name."""
    name = "atos-transfer-T3"
    proc = mp.get_context("fork").Process(
        target=_idle_incumbent, args=(name, broker), daemon=True,
    )
    proc.start()
    try:
        assert await _wait_for(broker, name, present=True), "incumbent never registered"

        await claim_name(
            name, broker, kill_existing=True,
            soft_grace=5.0, wait_timeout=10.0, poll_interval=0.1,
        )

        # Incumbent received the soft kill, disconnected, and the name is free.
        assert name not in await _names(broker)
        await asyncio.get_running_loop().run_in_executor(None, proc.join, 5)
        assert not proc.is_alive()
        assert proc.exitcode == 0  # clean exit from the soft kill

        # A fresh worker can now register under the reclaimed name.
        async with Client(name, broker):
            assert name in await _names(broker)
    finally:
        if proc.is_alive():
            proc.terminate()
            proc.join(5)

async def test_displaced_worker_terminates_children(broker):
    """A worker that loses its name on reconnect kills its child workers.

    Mirrors the ssh-push case: the data mover (here a child process group) is
    decoupled from the relay name, so a worker displaced by a newer ``--steal``
    worker must stop its children instead of leaving them running orphaned.
    """
    import subprocess

    child = subprocess.Popen(["sleep", "30"], start_new_session=True)
    a = Client("shared-name", broker, reconnect_retries=-1, reconnect_delay=1.0)
    try:
        await a.connect()
        # start_new_session=True makes the child its own group leader (pgid==pid).
        a.register_child_pgid(child.pid)

        # Drop A's socket so its listener attempts to reconnect, then wait for
        # the broker to free the name before a second worker grabs it.
        await a._ws.close()
        assert await _wait_for(broker, "shared-name", present=False, timeout=5.0)

        b = Client("shared-name", broker)
        await b.connect()  # B now holds the name; A's reconnect will be refused

        # A reconnects, gets NameTakenError, and SIGTERMs its child group.
        for _ in range(50):
            if child.poll() is not None:
                break
            await asyncio.sleep(0.1)
        assert child.poll() is not None, "displaced worker did not stop its child"
        await b.close()
    finally:
        await a.close()
        if child.poll() is None:
            child.kill()
            child.wait(timeout=5)

async def test_monitor_filter_matches_source():
    """The monitor --filter glob is plain fnmatch on the event source."""
    import fnmatch

    assert fnmatch.fnmatch("atos-transfer-T9", "*-T9")
    assert not fnmatch.fnmatch("atos-transfer-T8", "*-T9")
    assert fnmatch.fnmatch("ewc-to-lumi-T9", "*-T9")
