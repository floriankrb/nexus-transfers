"""Shared fixtures for nexus-transfers tests."""


import pytest
from websockets.asyncio.server import serve

from nexus_transfers.broker import (
    clients,
    clients_lock,
    monitors,
    monitors_lock,
    pending_calls,
    pending_calls_lock,
    relay_handler,
)


@pytest.fixture()
def free_port():
    """Return a free TCP port."""
    import socket

    with socket.socket() as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


@pytest.fixture()
async def broker(free_port):
    """Start a relay broker on a free port and yield the URL."""
    with clients_lock:
        clients.clear()
    with monitors_lock:
        monitors.clear()
    with pending_calls_lock:
        pending_calls.clear()

    async with serve(relay_handler, "localhost", free_port):
        yield f"ws://localhost:{free_port}"
