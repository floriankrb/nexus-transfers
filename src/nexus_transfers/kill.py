"""Kill connected nexus-transfers clients by name or wildcard pattern.

Usage::

    nexus-transfers kill <client-name>        # exact name
    nexus-transfers kill 'copy-*'             # shell-style wildcard
    nexus-transfers kill 'worker-?'           # single-character wildcard
    nexus-transfers kill --all                # every client

The pattern uses :mod:`fnmatch` semantics, so ``*`` matches any run of
characters and ``?`` matches a single character.  Quote the pattern in the
shell to stop it being expanded against local filenames.

Signals mirror ``kill(1)``:

* ``-9`` hard kill — the target exits immediately (``os._exit``), abandoning
  any in-flight transfer.
* ``-1`` soft kill — the target closes its connection cleanly and exits 0.

With neither flag the default is graceful escalation: send ``-1``, wait
``--grace`` seconds, then ``-9`` any client still connected.

A one-shot kill only reaches clients registered on the broker at that
instant: a worker that is mid-reconnect (e.g. a phantom from a dead SLURM
job in its retry loop) is invisible and survives. ``--sweep SECS`` repeats
the list-and-kill pass every ``--every`` seconds for the whole window, so
such a worker is caught the moment it re-registers::

    nexus-transfers kill '*-<task-id>' --sweep 30 --every 2
"""

import argparse
import asyncio
import fnmatch
import logging
import sys
import uuid

from nexus_transfers.client import Client

logger = logging.getLogger(__name__)


def _select_targets(clients, name, pattern, include_monitors):
    """Return the client names to kill.

    Parameters
    ----------
    clients
        All currently connected client names.
    name
        This killer client's own name, always excluded.
    pattern
        Wildcard pattern (``fnmatch`` syntax), or ``None`` to match every
        client (``--all``).
    include_monitors
        If False, ``monitor-*`` clients are skipped.
    """
    targets = []
    for c in clients:
        if c == name:
            # Never kill ourselves.
            continue
        if not include_monitors and c.startswith("monitor-"):
            continue
        if pattern is not None and not fnmatch.fnmatch(c, pattern):
            continue
        targets.append(c)
    return targets


async def _send_kill(client, targets, reason, signal):
    """Send a kill at *signal* to every target concurrently.

    Returns the number of targets that did not acknowledge.
    """
    failures = 0

    async def _one(t):
        nonlocal failures
        try:
            res = await client.kill(t, reason=reason, timeout=5.0, signal=signal)
            print(f"  ✓ {t}: {res}")
        except asyncio.TimeoutError:
            failures += 1
            print(f"  ✗ {t}: no ack (timeout)")
        except Exception as exc:
            failures += 1
            print(f"  ✗ {t}: {exc}")

    await asyncio.gather(*(_one(t) for t in targets))
    return failures


async def _kill_pass(client, name, targets, reason, mode, grace,
                     pattern, include_monitors):
    """Kill *targets* according to *mode*; return the number of failures."""
    if mode == "hard":
        return await _send_kill(client, targets, reason, 9)
    if mode == "soft":
        return await _send_kill(client, targets, reason, 1)

    # Default: soft first, then hard on whoever is still connected.
    await _send_kill(client, targets, reason, 1)
    print(f"Waiting {grace:g}s for clients to shut down …")
    await asyncio.sleep(grace)
    still_here = _select_targets(
        await client.list_clients(), name, pattern, include_monitors)
    survivors = [t for t in targets if t in still_here]
    if not survivors:
        print("All targets shut down after soft kill.")
        return 0
    print(f"{len(survivors)} client(s) still alive — sending hard kill (-9):")
    for t in survivors:
        print(f"  {t}")
    return await _send_kill(client, survivors, reason, 9)


async def _run(pattern, reason, include_monitors, dry_run, mode, grace,
               sweep=0.0, every=2.0):
    """Connect, resolve targets, and kill them. Return the exit code.

    With ``sweep > 0`` the list-and-kill pass repeats every ``every`` seconds
    until the window closes, catching workers that were disconnected (e.g.
    mid-reconnect) during earlier passes. The exit code then reflects the
    state at the end of the sweep: 0 when no matching client is left.
    """
    name = f"killer-{uuid.uuid4().hex[:6]}"
    async with Client(name) as client:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + sweep
        matched_any = False
        failures = 0
        while True:
            targets = _select_targets(
                await client.list_clients(), name, pattern, include_monitors)
            if targets:
                matched_any = True
                verb = {"soft": "Soft-killing (-1)", "hard": "Hard-killing (-9)",
                        "default": "Killing"}[mode]
                print(f"{verb} {len(targets)} client(s):")
                for t in targets:
                    print(f"  {t}")
                if dry_run:
                    print("(dry run — nothing sent)")
                else:
                    failures = await _kill_pass(
                        client, name, targets, reason, mode, grace,
                        pattern, include_monitors)
            if dry_run or loop.time() >= deadline:
                break
            await asyncio.sleep(every)

        if not matched_any:
            print("No matching clients to kill.")
            return 0
        if sweep > 0 and not dry_run:
            leftovers = _select_targets(
                await client.list_clients(), name, pattern, include_monitors)
            if leftovers:
                print(f"Sweep over — {len(leftovers)} matching client(s) "
                      "still connected.")
                return 1
            print("Sweep over — no matching clients left.")
            return 0
        return 1 if failures else 0


def main() -> None:
    """CLI entry point: ``nexus-transfers kill <pattern> | --all``."""
    parser = argparse.ArgumentParser(
        prog="nexus-transfers kill",
        description="Kill connected nexus-transfers clients by name or "
                    "wildcard pattern (use --all for every client).",
    )
    parser.add_argument(
        "pattern", nargs="?", default=None,
        help="Client name or wildcard pattern (* and ? supported). "
             "Quote it to avoid shell glob expansion.",
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Kill every connected client (equivalent to the pattern '*').",
    )
    sig = parser.add_mutually_exclusive_group()
    sig.add_argument(
        "-1", "--soft", dest="soft", action="store_true",
        help="Soft kill only: target closes cleanly and exits 0.",
    )
    sig.add_argument(
        "-9", "--hard", dest="hard", action="store_true",
        help="Hard kill only: target exits immediately, abandoning transfers.",
    )
    parser.add_argument(
        "--grace", type=float, default=2.0, metavar="SECS",
        help="Seconds to wait after the soft kill before escalating to a "
             "hard kill (default: 2.0; only used when neither -1 nor -9 given).",
    )
    parser.add_argument(
        "--sweep", type=float, default=0.0, metavar="SECS",
        help="Repeat the list-and-kill pass for this many seconds, so a "
             "worker that was mid-reconnect (and therefore invisible) during "
             "one pass is caught when it re-registers (default: 0 = single "
             "pass).",
    )
    parser.add_argument(
        "--every", type=float, default=2.0, metavar="SECS",
        help="Seconds between sweep passes (default: 2.0; only used with "
             "--sweep).",
    )
    parser.add_argument(
        "--reason", default="killed via nexus-transfers kill",
        help="Reason string logged by the target before it exits.",
    )
    parser.add_argument(
        "--include-monitors", action="store_true",
        help="Also kill monitor-* clients.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="List targets without killing them.",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Enable debug logging.",
    )
    args = parser.parse_args()

    if args.all and args.pattern is not None:
        parser.error("give either a pattern or --all, not both")
    if not args.all and args.pattern is None:
        parser.error("provide a client name/pattern, or pass --all")

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    mode = "soft" if args.soft else "hard" if args.hard else "default"
    # --all matches everything; a pattern restricts the selection.
    pattern = None if args.all else args.pattern
    rc = asyncio.run(_run(
        pattern, args.reason, args.include_monitors, args.dry_run,
        mode, args.grace, sweep=args.sweep, every=args.every,
    ))
    sys.exit(rc)


if __name__ == "__main__":
    main()
