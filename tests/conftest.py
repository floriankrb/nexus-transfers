"""Shared fixtures for nexus-transfers tests."""

import asyncio

import pytest
from websockets.asyncio.server import serve

from nexus_transfers.server import relay_handler, clients, clients_lock, shared_memory, shared_memory_lock


@pytest.fixture()
def free_port():
    """Return a free TCP port."""
    import socket

    with socket.socket() as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


@pytest.fixture()
async def server(free_port):
    """Start a relay server on a free port and yield the URL."""
    # Clear global state from previous tests
    with clients_lock:
        clients.clear()
    with shared_memory_lock:
        shared_memory.clear()

    async with serve(relay_handler, "localhost", free_port):
        yield f"ws://localhost:{free_port}"
