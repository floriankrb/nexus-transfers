"""Shared plumbing of the copy and check commands: best-effort monitor
events, bounded worker pools and periodic tickers."""

import asyncio
import contextlib
import logging
from typing import Awaitable, Callable

_logger = logging.getLogger(__name__)

_DONE = object()


class Monitor:
    """Best-effort monitor events, sent to the relay and to an ``on_monitor``
    callback.

    A failure of either is logged and never interrupts the work.  Use as an
    async context manager (or call :meth:`close`) to release the relay
    connection.

    Parameters
    ----------
    client : nexus_transfers.client.Client or None
        Connected relay client; None sends nothing to the relay.
    on_monitor : callable, optional
        Async callback invoked with ``(message, status=..., **kwargs)`` for
        every event.
    """

    def __init__(self, client=None, on_monitor: Callable | None = None) -> None:
        self.client = client
        self._on_monitor = on_monitor

    @classmethod
    async def connect(cls, name: str, broker_url: str | None, *,
                      ssl_verify: bool = True, steal: bool = False,
                      on_monitor: Callable | None = None) -> "Monitor":
        """Register *name* on the broker (see
        :func:`~nexus_transfers.claim.connect_locked` for *steal*)."""
        # Lazy: claim imports the client package, which uses run_workers.
        from nexus_transfers.claim import connect_locked

        client = await connect_locked(name, broker_url,
                                      ssl_verify=ssl_verify, steal=steal)
        return cls(client, on_monitor)

    async def emit(self, message: str, status: str | None = None, **kw) -> None:
        """Send one monitor event."""
        if self.client is not None:
            try:
                await self.client.monitor(message, status=status, **kw)
            except Exception as exc:
                _logger.warning("Failed to send monitor event: %s", exc)
        if self._on_monitor is not None:
            try:
                await self._on_monitor(message, status=status, **kw)
            except Exception as exc:
                _logger.warning("on_monitor callback failed: %s", exc)

    async def close(self) -> None:
        """Close the relay connection (idempotent)."""
        client, self.client = self.client, None
        if client is not None:
            await client.close()

    async def __aenter__(self) -> "Monitor":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()


async def run_workers(
    produce,
    work: Callable[[object], Awaitable[None]],
    n: int,
    *,
    maxsize: int = 0,
) -> None:
    """Run ``await work(item)`` over the produced items in *n* workers.

    *produce* is an iterable or async iterable of items, or an async
    callable ``produce(put)`` that calls ``await put(item)`` for each item
    (for recursive walks).  Items flow through a queue of *maxsize* (0:
    unbounded), so a bounded queue keeps huge trees out of memory.

    The first failure — of the producer or any worker — cancels everything
    else and propagates unchanged.
    """
    n = max(1, n)
    queue: asyncio.Queue = asyncio.Queue(maxsize)

    async def _produce() -> None:
        if callable(produce):
            await produce(queue.put)
        elif hasattr(produce, "__aiter__"):
            async for item in produce:
                await queue.put(item)
        else:
            for item in produce:
                await queue.put(item)
        for _ in range(n):
            await queue.put(_DONE)

    async def _worker() -> None:
        while (item := await queue.get()) is not _DONE:
            await work(item)

    tasks = [asyncio.create_task(_produce()),
             *[asyncio.create_task(_worker()) for _ in range(n)]]
    try:
        await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


@contextlib.asynccontextmanager
async def ticking(interval: float, tick: Callable[[], Awaitable[None]]):
    """Call ``await tick()`` every *interval* seconds while the block runs."""
    stop = asyncio.Event()

    async def _loop() -> None:
        while True:
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
                return
            except asyncio.TimeoutError:
                await tick()

    task = asyncio.create_task(_loop())
    try:
        yield
    finally:
        stop.set()
        await task
