"""Kill connected nexus-transfers clients by name or wildcard pattern.

Usage::

    nexus-transfers kill <client-name>        # exact name
    nexus-transfers kill 'copy-*'             # shell-style wildcard
    nexus-transfers kill 'worker-?'           # single-character wildcard
    nexus-transfers kill --all                # every client

The pattern uses :mod:`fnmatch` semantics, so ``*`` matches any run of
characters and ``?`` matches a single character.  Quote the pattern in the
shell to stop it being expanded against local filenames.
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


async def _run(pattern, reason, include_monitors, dry_run):
    """Connect, resolve targets, and kill them concurrently. Return exit code."""
    name = f"killer-{uuid.uuid4().hex[:6]}"
    async with Client(name) as client:
        clients = await client.list_clients()
        targets = _select_targets(clients, name, pattern, include_monitors)
        if not targets:
            print("No matching clients to kill.")
            return 0

        print(f"Killing {len(targets)} client(s):")
        for t in targets:
            print(f"  {t}")
        if dry_run:
            print("(dry run — nothing sent)")
            return 0

        failures = 0

        async def _kill(t):
            nonlocal failures
            try:
                res = await client.kill(t, reason=reason, timeout=5.0)
                print(f"  ✓ {t}: {res}")
            except asyncio.TimeoutError:
                failures += 1
                print(f"  ✗ {t}: no ack (timeout)")
            except Exception as exc:
                failures += 1
                print(f"  ✗ {t}: {exc}")

        await asyncio.gather(*(_kill(t) for t in targets))
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

    # --all matches everything; a pattern restricts the selection.
    pattern = None if args.all else args.pattern
    rc = asyncio.run(_run(
        pattern, args.reason, args.include_monitors, args.dry_run,
    ))
    sys.exit(rc)


if __name__ == "__main__":
    main()
