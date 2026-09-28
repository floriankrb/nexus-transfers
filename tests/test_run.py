"""Tests for the shared run plumbing (``_run``)."""

import asyncio

import pytest

from nexus_transfers._run import Monitor, run_workers, ticking


async def test_run_workers_iterable():
    seen = []

    async def work(item):
        seen.append(item)

    await run_workers(range(10), work, 3, maxsize=2)
    assert sorted(seen) == list(range(10))


async def test_run_workers_async_iterable_and_put_callable():
    async def items():
        for i in range(5):
            yield i

    async def produce(put):
        for i in range(5, 10):
            await put(i)

    seen = []

    async def work(item):
        seen.append(item)

    await run_workers(items(), work, 2)
    await run_workers(produce, work, 2)
    assert sorted(seen) == list(range(10))


async def test_run_workers_failure_cancels_the_rest():
    started = asyncio.Event()
    cancelled = []

    async def work(item):
        if item == 0:
            await started.wait()
            raise KeyError("boom")
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.append(item)
            raise

    with pytest.raises(KeyError):          # unwrapped, not an ExceptionGroup
        await asyncio.wait_for(run_workers(range(2), work, 2), 5)
    assert cancelled == [1]


async def test_run_workers_producer_failure_propagates():
    async def produce(put):
        await put(1)
        raise FileNotFoundError("nothing there")

    async def work(item):
        await asyncio.sleep(0)

    with pytest.raises(FileNotFoundError):
        await asyncio.wait_for(run_workers(produce, work, 2, maxsize=1), 5)


async def test_monitor_is_best_effort():
    class Broken:
        async def monitor(self, *a, **kw):
            raise ConnectionError("relay down")

        async def close(self):
            pass

    events = []

    async def on_monitor(message, status=None, **kw):
        events.append((message, status, kw))

    async with Monitor(Broken(), on_monitor) as monitor:
        await monitor.emit("hello", status="progress", progress={"value": 1})
    assert events == [("hello", "progress", {"progress": {"value": 1}})]
    assert monitor.client is None


async def test_ticking():
    ticks = []

    async def tick():
        ticks.append(1)

    async with ticking(0.01, tick):
        await asyncio.sleep(0.1)
    n = len(ticks)
    assert n >= 3
    await asyncio.sleep(0.05)
    assert len(ticks) == n                 # stopped with the block
