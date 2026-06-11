"""Shared fixtures for nexus-transfers tests."""

import os

import pytest
from websockets.asyncio.server import serve

from nexus_transfers import config as _config
from nexus_transfers.broker import (
    clients,
    clients_lock,
    monitors,
    monitors_lock,
    pending_calls,
    pending_calls_lock,
    relay_handler,
)


@pytest.fixture(autouse=True)
def _isolate_from_real_config(monkeypatch):
    """Keep tests away from the developer's real environment.

    ``client._client`` and ``s3`` load ``~/.env`` at import time, so a
    developer's real S3 credentials and broker settings would otherwise leak
    into the test run (and tests could write to a real bucket).  Scrub all
    ``NEXUS_TRANSFER*`` variables and blank the TOML config cache.
    """
    for var in [v for v in os.environ if v.startswith("NEXUS_TRANSFER")]:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr(_config, "_config", {})


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
