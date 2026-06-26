"""Claim a client name on the broker before registering under it.

The broker enforces globally-unique client names: a second client that tries
to register an in-use name is rejected.  That makes the name itself a
distributed lock — exactly one peer can hold ``name`` at a time.  This module
lets a starting worker *take over* a name that a previous (possibly stuck)
worker still holds, by killing the incumbent and waiting for it to drop before
the caller registers.

The helper is deliberately unaware of any "task" concept: it operates purely on
a client *name*.  Callers that key names on a task id (e.g.
``"<site>-transfer-<task-id>"``) get a per-task interlock for free.

No broker change is involved — the kill travels over the existing broker relay
and the name is freed atomically when the incumbent disconnects.  The caller
then registers under ``name`` itself; if a racing claimant grabbed the name in
between, the caller's own registration fails with
:class:`~nexus_transfers.client.NameTakenError`, so at most one worker ever
holds the name.
"""

import asyncio
import logging
import uuid

from nexus_transfers.client import Client, NameTakenError

_logger = logging.getLogger(__name__)

async def _connect_with_retry(client, *, connect_retries, connect_delay):
    """Connect *client*, waiting for the broker if it is unreachable.

    ``connect_retries`` of ``-1`` means retry forever — so a broker outage at
    startup makes the worker *wait* for the broker rather than starting
    unguarded (no duplicate-detection means no concurrency guarantee).
    """
    attempt = 0
    while True:
        try:
            await client.connect()
            return
        except NameTakenError:
            # Our own short-lived claim name collided — vanishingly unlikely
            # given the random suffix, but treat it as a transient and retry.
            raise
        except Exception as exc:
            attempt += 1
            if connect_retries != -1 and attempt > connect_retries:
                raise
            _logger.warning(
                "Broker unreachable while claiming a name (%s); retry %d in %.1fs",
                exc, attempt, connect_delay,
            )
            await asyncio.sleep(connect_delay)

async def _wait_until_gone(client, name, *, timeout, poll_interval):
    """Poll the broker until *name* is no longer connected, or *timeout* elapses.

    Returns True if the name is gone, False if it is still present at timeout.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        if name not in await client.list_clients():
            return True
        if loop.time() >= deadline:
            return False
        await asyncio.sleep(poll_interval)

async def claim_name(
    name,
    broker_url=None,
    *,
    ssl_verify=True,
    kill_existing=False,
    reason="displaced by a newer worker for the same task",
    soft_grace=5.0,
    wait_timeout=30.0,
    poll_interval=0.5,
    connect_retries=-1,
    connect_delay=2.0,
):
    """Ensure *name* is free on the broker so the caller can register under it.

    Connects a short-lived client, lists the connected peers, and:

    * if *name* is not connected, returns immediately — the name is free;
    * if *name* is connected and ``kill_existing`` is False, raises
      :class:`~nexus_transfers.client.NameTakenError`;
    * if *name* is connected and ``kill_existing`` is True, sends a soft kill,
      waits up to ``soft_grace`` for the incumbent to drop, escalates to a hard
      kill for a survivor, and waits up to ``wait_timeout`` in total.

    This helper does **not** register *name* itself.  The caller registers
    afterwards and wins the name atomically at the broker; a claimant that lost
    a race gets ``NameTakenError`` on its own registration.

    Parameters
    ----------
    name
        Client name to claim (e.g. ``"atos-transfer-<task-id>"``).
    broker_url
        Broker WebSocket URL.  If ``None``, claiming is skipped (there is no
        broker to hold the lock) and the function returns immediately.
    ssl_verify
        Verify the TLS certificate for ``wss://`` brokers.
    kill_existing
        If True, displace an incumbent holder of *name*.  If False, an
        incumbent causes ``NameTakenError``.
    reason
        Reason string logged by the incumbent before it exits.
    soft_grace
        Seconds to wait after the soft kill before escalating to a hard kill.
    wait_timeout
        Total seconds to wait for *name* to become free.
    poll_interval
        Seconds between broker polls while waiting for the name to drop.
    connect_retries
        Broker-connect attempts for the short-lived claim client (``-1`` =
        infinite, so startup waits for a down broker rather than running
        unguarded).
    connect_delay
        Seconds between connect attempts.

    Raises
    ------
    NameTakenError
        If *name* is held and ``kill_existing`` is False, or if the incumbent
        could not be displaced within ``wait_timeout``.
    """
    if not broker_url:
        _logger.debug("No broker URL; skipping name claim for %r", name)
        return

    claimer = f"claim-{uuid.uuid4().hex[:8]}"
    client = Client(claimer, broker_url, ssl_verify=ssl_verify)
    await _connect_with_retry(
        client, connect_retries=connect_retries, connect_delay=connect_delay,
    )
    try:
        if name not in await client.list_clients():
            _logger.debug("Name %r is free", name)
            return

        if not kill_existing:
            raise NameTakenError(
                f"name {name!r} is already held; pass kill_existing=True to displace it"
            )

        _logger.warning(
            "Name %r already held — displacing the incumbent worker", name,
        )
        # Soft kill first so the incumbent can tear down its SSH children
        # cleanly, then escalate to a hard kill for a survivor.
        try:
            await client.kill(name, reason=reason, signal=1)
        except asyncio.TimeoutError:
            _logger.warning("No ack for soft kill of %r (already gone?)", name)
        except Exception as exc:
            _logger.warning("Soft kill of %r failed: %s", name, exc)

        if await _wait_until_gone(
            client, name, timeout=soft_grace, poll_interval=poll_interval,
        ):
            _logger.info("Incumbent %r shut down after soft kill", name)
            return

        _logger.warning("Incumbent %r still connected — sending hard kill", name)
        try:
            await client.kill(name, reason=reason, signal=9)
        except asyncio.TimeoutError:
            _logger.warning("No ack for hard kill of %r", name)
        except Exception as exc:
            _logger.warning("Hard kill of %r failed: %s", name, exc)

        remaining = max(wait_timeout - soft_grace, poll_interval)
        if not await _wait_until_gone(
            client, name, timeout=remaining, poll_interval=poll_interval,
        ):
            raise NameTakenError(
                f"could not displace incumbent holder of {name!r} "
                f"within {wait_timeout:g}s"
            )
        _logger.info("Incumbent %r gone after hard kill", name)
    finally:
        await client.close()
