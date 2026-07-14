"""CLI tool: verify a remote SSH copy against the local reference.

The local directory is the reference. Local files are walked and hashed,
the remote hash is computed over SSH (``md5sum`` by default, same asyncssh
pool as ``nexus-copy-ssh``), and the two are compared.

Usage::

    nexus-transfers check-files-ssh --source /data/dataset.zarr --target user@host:/remote/path
    nexus-transfers check-files-ssh --source ... --target ... --fix --delete-extra
"""

import argparse
import asyncio
import logging
import os
import stat as _stat
import sys
import time
import uuid
from typing import Callable

from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
)

from nexus_transfers._progress import (
    _BinarySpeedColumn,
    _CountOrBytesColumn,
    make_console,
    setup_cli_logging,
)
from nexus_transfers.check_files import (
    CheckFailedError,
    CheckReport,
    _parse_age,
    _parse_mode,
    is_deletable_extra,
    scan_local_files,
)
from nexus_transfers.client import Client
from nexus_transfers.config import cli_default
from nexus_transfers.copy_ssh import _parse_target
from nexus_transfers.dispatch import compute_file_hash
from nexus_transfers.ssh import (
    SSHPool,
    remote_hash,
    walk_remote,
    write_file,
)

_logger = logging.getLogger(__name__)


async def _check_one_ssh(pool, local_root, remote_base, rel, report, *,
                         algo, fix, fix_permissions, max_age, loop):
    """Compare one local reference file against its remote counterpart.

    Returns True when the file was checked, False when it was skipped
    because the remote copy is older than *max_age*.

    Parameters
    ----------
    pool : SSHPool
        Connection pool to draw SSH/SFTP clients from.
    local_root : str
        Local reference root directory.
    remote_base : str
        Remote root directory (POSIX).
    rel : str
        File path relative to both roots (POSIX).
    report : CheckReport
        Aggregator for discrepancies.
    algo : str
        Hash algorithm (must have a ``<algo>sum`` binary on the remote).
    fix : bool
        Re-upload corrupt or missing remote files.
    fix_permissions : int or None
        Explicit permission bits (e.g. ``0o600``) to enforce on every remote
        file. ``None`` only reports drift against the local reference.
    max_age : float or None
        Only check remote files modified within the last ``max_age``
        seconds; older files are skipped. Missing remote files are never
        skipped — they have no mtime and must be reported.
    loop :
        Running event loop (for executor calls).
    """
    local_file = os.path.join(local_root, rel)
    remote_file = f"{remote_base}/{rel}"
    sftp = pool.get_sftp()
    local_mode = _stat.S_IMODE(os.stat(local_file).st_mode)

    if max_age is not None:
        try:
            attrs = await sftp.stat(remote_file)
        except Exception:
            attrs = None  # missing remote file: never skip, check it
        if (attrs is not None and attrs.mtime is not None
                and time.time() - attrs.mtime > max_age):
            report.skipped += 1
            return False

    remote_digest, local_digest = await asyncio.gather(
        remote_hash(pool.get_conn(), remote_file, algo=algo),
        loop.run_in_executor(None, compute_file_hash, local_file, algo),
    )

    content_fixed = False
    if remote_digest is None:
        fix_label = None
        if fix:
            await write_file(sftp, local_file, remote_file)
            # A repaired file must fully match the reference, mode
            # included (a fresh upload gets server-default bits).
            await sftp.chmod(remote_file, local_mode)
            fix_label = "uploaded"
            content_fixed = True
        report.add("missing", rel, "not found on remote", fix=fix_label)
    elif remote_digest != local_digest:
        fix_label = None
        if fix:
            await write_file(sftp, local_file, remote_file)
            await sftp.chmod(remote_file, local_mode)
            fix_label = "re-uploaded"
            content_fixed = True
        report.add(
            "corrupt", rel,
            f"{algo} remote {remote_digest} != local {local_digest}",
            fix=fix_label,
        )

    if remote_digest is None and not content_fixed:
        return True  # nothing on the remote to compare permissions against

    try:
        attrs = await sftp.stat(remote_file)
    except Exception:
        return True
    remote_mode = _stat.S_IMODE(attrs.permissions)
    if fix_permissions is not None:
        # Explicit target mode: enforce it on the remote copy.
        if remote_mode != fix_permissions:
            await sftp.chmod(remote_file, fix_permissions)
            report.add(
                "mode", rel,
                f"remote {remote_mode:o} != required {fix_permissions:o}",
                fix=f"chmod {fix_permissions:o}",
            )
    elif remote_mode != local_mode:
        # Detection only: report drift against the local reference.
        report.add(
            "mode", rel,
            f"remote {remote_mode:o} != local {local_mode:o}",
        )
    return True


async def _check_ssh(
    source: str,
    target: str,
    broker_url: str | None,
    name: str,
    site: str | None,
    *,
    algo: str = "md5",
    fix: bool = False,
    delete_extra: bool = False,
    fix_permissions: int | None = None,
    max_concurrent: int = 4,
    ssh_port: int = 22,
    ssh_key: str | None = None,
    ssh_connections: int = 2,
    ssl_verify: bool = True,
    encryption_algs: list[str] | None = None,
    max_age: float | None = None,
    on_monitor: Callable | None = None,
    quiet: bool = False,
) -> CheckReport:
    """Verify the SSH *target* against the local *source* reference.

    Parameters
    ----------
    source : str
        Local reference directory.
    target : str
        Remote copy in the form ``[user@]host:/path``.
    broker_url : str or None
        Relay WebSocket URL for monitoring only; ``None`` disables monitoring.
    name : str
        Client name on the relay.
    site : str or None
        Site label for monitor messages.
    algo : str
        Hash algorithm; the remote host needs a ``<algo>sum`` binary.
    fix : bool
        Re-upload corrupt or missing remote files instead of failing.
    delete_extra : bool
        Delete whitelisted extra remote files (failed-transfer leftovers
        and ``_build/*``, see :func:`is_deletable_extra`); other extras
        are only reported, never deleted.
    fix_permissions : int or None
        Explicit permission bits (e.g. ``0o600``) to enforce on every remote
        file; None (default) only reports drift against the local reference.
    max_concurrent : int
        Maximum number of files checked in parallel.
    ssh_port : int
        SSH port on the target host.
    ssh_key : str or None
        Path to the SSH private key file.
    ssh_connections : int
        Number of SSH connections to open in the pool.
    ssl_verify : bool
        Verify TLS certificate for the relay connection.
    encryption_algs : list of str or None
        SSH cipher preference list.
    max_age : float or None
        Only check remote files modified within the last ``max_age``
        seconds; older files are skipped. None (default) checks everything.
    on_monitor : callable, optional
        Async callback invoked with ``(message, status=..., **kwargs)`` for
        every monitor event.
    quiet : bool
        If True, suppress rich console output (monitor events still fire).

    Returns
    -------
    CheckReport
        The filled-in report; ``report.ok`` is False when unfixed
        discrepancies remain.
    """
    user, host, remote_base = _parse_target(target)
    source = os.path.expanduser(source)
    remote_base = remote_base.rstrip("/")
    # The local tree is the reference here: a missing or empty source is
    # far more likely a typo or filesystem problem than a real dataset,
    # and with --delete-extra it would classify the entire remote tree as
    # extra. Refuse before touching anything.
    if not os.path.isdir(source):
        raise CheckFailedError(
            f"reference {source} is not a directory — refusing to check"
        )
    console = make_console(quiet=quiet)
    loop = asyncio.get_running_loop()

    label = os.path.basename(source.rstrip("/")) or source
    dest_label = f"{site}:{remote_base}" if site else f"{host}:{remote_base}"
    if not quiet:
        console.print(
            f"Checking [yellow]{dest_label}[/yellow] against "
            f"[yellow]{source}[/yellow]"
        )

    monitor_client: Client | None = None
    if broker_url:
        try:
            monitor_client = Client(
                name, broker_url, dispatch={},
                ssl_verify=ssl_verify, reconnect_retries=-1,
            )
            await monitor_client.connect()
        except Exception as exc:
            _logger.warning(
                "Relay unavailable (%s), continuing without monitor", exc,
            )
            monitor_client = None

    async def _emit(message, status=None, **kw):
        if monitor_client is not None:
            try:
                await monitor_client.monitor(message, status=status, **kw)
            except Exception as exc:
                _logger.warning("Failed to send monitor event: %s", exc)
        if on_monitor is not None:
            try:
                await on_monitor(message, status=status, **kw)
            except Exception as exc:
                _logger.warning("on_monitor callback failed: %s", exc)

    await _emit(
        f"{name}: starting check {dest_label} against {source}",
        status="progress",
    )

    report = CheckReport(_emit, name, label)

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        _CountOrBytesColumn(),
        _BinarySpeedColumn(),
        TimeRemainingColumn(),
        transient=True,
        disable=quiet,
    )
    progress.start()
    walk_task_id = progress.add_task(
        f"[magenta]Listing {label}[/magenta]", total=None, unit="files",
    )
    local_files = await loop.run_in_executor(None, scan_local_files, source)
    if not local_files:
        progress.stop()
        raise CheckFailedError(
            f"reference {source} contains no files — refusing to check "
            "(wrong path or filesystem issue?)"
        )
    report.total = len(local_files)
    progress.remove_task(walk_task_id)
    check_task_id = progress.add_task(
        f"[cyan]Checking {label}[/cyan]", total=len(local_files), unit="files",
    )

    try:
        async with SSHPool(
            host, ssh_port, user, ssh_key, ssh_connections, encryption_algs,
        ) as pool:

            # Bounded queue + workers rather than one task per file: zarr
            # trees can hold 100k+ chunk files.
            queue: asyncio.Queue = asyncio.Queue(maxsize=max_concurrent * 4)

            async def _producer() -> None:
                for rel in sorted(local_files):
                    await queue.put(rel)
                for _ in range(max_concurrent):
                    await queue.put(None)

            async def _worker() -> None:
                while True:
                    rel = await queue.get()
                    if rel is None:
                        return
                    checked = await _check_one_ssh(
                        pool, source, remote_base, rel, report,
                        algo=algo, fix=fix, fix_permissions=fix_permissions,
                        max_age=max_age, loop=loop,
                    )
                    if checked:
                        report.checked += 1
                    progress.update(
                        check_task_id,
                        completed=report.checked + report.skipped,
                    )
                    await report.maybe_report()

            await asyncio.gather(
                _producer(), *[_worker() for _ in range(max_concurrent)],
            )

            # Extra remote files (the local copy is the reference).
            remote_files = await walk_remote(pool.get_sftp(), remote_base)
            for rel, _size in sorted(remote_files):
                if rel in local_files:
                    continue
                fix_label = None
                detail = "not in the local reference"
                if delete_extra:
                    # Only ever delete whitelisted extras (failed-transfer
                    # debris, _build/*); anything else is kept and reported.
                    if is_deletable_extra(rel, local_files):
                        try:
                            await pool.get_sftp().remove(f"{remote_base}/{rel}")
                            fix_label = "deleted"
                        except Exception as exc:
                            _logger.warning(
                                "Could not delete extra remote file %s: %s",
                                rel, exc,
                            )
                    else:
                        detail += " (kept: not a deletable extra)"
                report.add("extra", rel, detail, fix=fix_label)
                await report.maybe_report()
    finally:
        progress.stop()

    await report.final_report()
    report.print_summary(console)

    if monitor_client:
        await monitor_client.close()
    return report


def main() -> None:
    """CLI entry point for ``nexus-transfers check-files-ssh``."""
    parser = argparse.ArgumentParser(
        description="Verify a remote SSH copy against the local reference "
                    "(hashes and permissions), optionally fixing it",
    )
    parser.add_argument("--source", required=True,
                        help="Local reference directory")
    parser.add_argument(
        "--target", required=True,
        help="Remote copy to verify: [user@]host:/remote/path",
    )
    parser.add_argument(
        "--broker-url",
        default=cli_default("broker_url", "check_files_ssh", default=None),
        help="Relay WebSocket URL for monitoring (default: none — monitoring disabled)",
    )
    parser.add_argument(
        "--name", default=cli_default("name", "check_files_ssh", default=None),
        help="Client name on the relay (default: auto-generated)",
    )
    parser.add_argument(
        "--site", default=cli_default("site", "check_files_ssh", default=None),
        help="Site label for monitor messages",
    )
    parser.add_argument(
        "--algo", default=cli_default("algo", "check_files_ssh", default="md5"),
        help="Hash algorithm; the remote host needs <algo>sum (default: md5)",
    )
    parser.add_argument(
        "--fix", action="store_true",
        default=cli_default("fix", "check_files_ssh", default=False),
        help="Re-upload corrupt or missing remote files instead of failing",
    )
    parser.add_argument(
        "--delete-extra", action="store_true",
        default=cli_default("delete_extra", "check_files_ssh", default=False),
        help="Delete whitelisted extra remote files: failed-transfer "
             "leftovers (<base>.<hex>.tmp with <base> in the local "
             "reference) and files under _build/; other extras are only "
             "reported, never deleted",
    )
    parser.add_argument(
        "--fix-permissions", metavar="MODE", type=_parse_mode,
        default=cli_default("fix_permissions", "check_files_ssh", default=None,
                            type_fn=_parse_mode),
        help="Octal permission bits to enforce on every remote file "
             "(e.g. 600); without this option drift is only reported",
    )
    parser.add_argument(
        "--max-concurrent", type=int,
        default=cli_default("max_concurrent", "check_files_ssh", default=4,
                            type_fn=int),
        help="Maximum parallel file checks (default: 4)",
    )
    parser.add_argument(
        "--ssh-port", type=int,
        default=cli_default("ssh_port", "check_files_ssh", default=22,
                            type_fn=int),
        help="SSH port (default: 22)",
    )
    parser.add_argument(
        "--ssh-key",
        default=cli_default("ssh_key", "check_files_ssh", default=None),
        help="Path to SSH private key",
    )
    parser.add_argument(
        "--ssh-connections", type=int,
        default=cli_default("ssh_connections", "check_files_ssh", default=2,
                            type_fn=int),
        help="Number of SSH connections to open (default: 2)",
    )
    parser.add_argument(
        "--max-age", metavar="AGE", type=_parse_age,
        default=cli_default("max_age", "check_files_ssh", default=None,
                            type_fn=_parse_age),
        help="Only check remote files modified within AGE — e.g. 30d, 1h, "
             "45m, 4 (seconds); older files are skipped (default: check all)",
    )
    parser.add_argument(
        "--cipher", nargs="+", default=None, metavar="ALG",
        help="SSH cipher preference list (default: aes128-gcm@openssh.com first)",
    )
    parser.add_argument(
        "--no-verify", action="store_true",
        default=cli_default("no_verify", "check_files_ssh", default=False),
        help="Skip TLS verification for the relay connection",
    )
    parser.add_argument(
        "--debug", action="store_true",
        default=cli_default("debug", "check_files_ssh", default=False),
        help="Enable debug logging",
    )
    args = parser.parse_args()

    setup_cli_logging(debug=args.debug)

    # e.g. "lumi-00085e89-check-ssh": same prefix as the copy client of
    # that site, with a suffix telling the monitor what this client does.
    prefix = f"{args.site}-" if args.site else ""
    name = args.name or f"{prefix}{uuid.uuid4().hex[:8]}-check-ssh"

    try:
        report = asyncio.run(
            _check_ssh(
                source=args.source,
                target=args.target,
                broker_url=args.broker_url,
                name=name,
                site=args.site,
                algo=args.algo,
                fix=args.fix,
                delete_extra=args.delete_extra,
                fix_permissions=args.fix_permissions,
                max_concurrent=args.max_concurrent,
                ssh_port=args.ssh_port,
                ssh_key=args.ssh_key,
                ssh_connections=args.ssh_connections,
                ssl_verify=not args.no_verify,
                encryption_algs=args.cipher,
                max_age=args.max_age,
            )
        )
    except CheckFailedError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(2)
    if not report.ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
