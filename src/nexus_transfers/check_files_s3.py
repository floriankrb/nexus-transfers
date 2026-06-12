"""CLI tool: verify an S3 copy against the local reference.

The local directory is the reference. By default sizes are compared using a
single bucket listing; with ``--hash`` every object is streamed back from S3
and hashed, which re-downloads every byte but catches silent corruption.

Usage::

    nexus-transfers check-files-s3 --source /data/dataset.zarr --target s3://bucket/datasets/dataset.zarr
    nexus-transfers check-files-s3 --source ... --target ... --hash md5 --fix --delete-extra
"""

import argparse
import asyncio
import hashlib
import logging
import os
import sys
import uuid
from typing import Callable

import obstore as obs
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
)

from nexus_transfers import s3
from nexus_transfers._progress import (
    _BinarySpeedColumn,
    _CountOrBytesColumn,
    make_console,
    setup_cli_logging,
)
from nexus_transfers.check_files import (
    CheckFailedError,
    CheckReport,
    is_failed_transfer_leftover,
    scan_local_files,
)
from nexus_transfers.client import Client
from nexus_transfers.config import cli_default
from nexus_transfers.dispatch import compute_file_hash

_logger = logging.getLogger(__name__)


def _hash_remote(bucket: str, key: str, algo: str) -> str | None:
    """Stream an object from S3 and return its hex digest (None if missing)."""
    store = s3.get_store(bucket=bucket)
    hasher = hashlib.new(algo)
    try:
        result = obs.get(store, key)
        for chunk in result.stream():
            hasher.update(bytes(chunk))
    except FileNotFoundError:
        return None
    return hasher.hexdigest()


async def _check_one_s3(
    bucket: str,
    local_root: str,
    prefix: str | None,
    rel: str,
    local_size: int,
    remote_sizes: dict[str, int],
    report: CheckReport,
    *,
    algo: str | None,
    fix: bool,
    loop,
) -> None:
    """Compare one local reference file against its S3 counterpart.

    Parameters
    ----------
    bucket : str
        Bucket name.
    local_root : str
        Local reference root directory.
    prefix : str or None
        Key prefix of the S3 copy.
    rel : str
        File path relative to both roots (POSIX).
    local_size : int
        Size of the local reference file.
    remote_sizes : dict
        ``{rel: size}`` map from the bucket listing.
    report : CheckReport
        Aggregator for discrepancies.
    algo : str or None
        Hash algorithm; None compares sizes only.
    fix : bool
        Re-upload corrupt or missing objects.
    loop :
        Running event loop (for executor calls).
    """
    local_file = os.path.join(local_root, rel)
    key = f"{prefix}/{rel}" if prefix else rel

    async def _reupload() -> None:
        await loop.run_in_executor(
            None,
            lambda: s3.upload_file(local_file, s3_key=key, bucket=bucket),
        )

    if rel not in remote_sizes:
        fix_label = None
        if fix:
            await _reupload()
            fix_label = "uploaded"
        report.add("missing", rel, "not found on S3", fix=fix_label)
        return

    if algo is not None:
        remote_digest, local_digest = await asyncio.gather(
            loop.run_in_executor(None, _hash_remote, bucket, key, algo),
            loop.run_in_executor(None, compute_file_hash, local_file, algo),
        )
        if remote_digest == local_digest:
            return
        detail = f"{algo} remote {remote_digest} != local {local_digest}"
    else:
        remote_size = remote_sizes[rel]
        if remote_size == local_size:
            return
        detail = f"size remote {remote_size} != local {local_size}"

    fix_label = None
    if fix:
        await _reupload()
        fix_label = "re-uploaded"
    report.add("corrupt", rel, detail, fix=fix_label)


async def _check_s3(
    source: str,
    target: str,
    broker_url: str | None,
    name: str,
    site: str | None,
    *,
    algo: str | None = None,
    fix: bool = False,
    delete_extra: bool = False,
    max_concurrent: int = 8,
    ssl_verify: bool = True,
    on_monitor: Callable | None = None,
    quiet: bool = False,
) -> CheckReport:
    """Verify the S3 *target* against the local *source* reference.

    Parameters
    ----------
    source : str
        Local reference directory.
    target : str
        S3 copy to verify: ``s3://bucket[/prefix]``.
    broker_url : str or None
        Relay WebSocket URL for monitoring only; ``None`` disables monitoring.
    name : str
        Client name on the relay.
    site : str or None
        Site label for monitor messages.
    algo : str or None
        Hash algorithm (any :func:`hashlib.new` name). None (default)
        compares sizes only; hashing re-downloads every byte.
    fix : bool
        Re-upload corrupt or missing objects instead of failing.
    delete_extra : bool
        Delete extra objects that are failed-transfer leftovers
        (``<base>.<hex>[.tmp]`` with ``<base>`` in the local reference);
        other extras are only reported, never deleted.
    max_concurrent : int
        Maximum number of files checked in parallel.
    ssl_verify : bool
        Verify TLS certificate for the relay connection.
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
    bucket, prefix = s3.parse_s3_url(target)
    source = os.path.expanduser(source)
    # The local tree is the reference here: a missing or empty source is
    # far more likely a typo or filesystem problem than a real dataset,
    # and with --delete-extra it would classify every object under the
    # prefix as extra. Refuse before touching anything.
    if not os.path.isdir(source):
        raise CheckFailedError(
            f"reference {source} is not a directory — refusing to check"
        )
    console = make_console(quiet=quiet)
    loop = asyncio.get_running_loop()

    label = os.path.basename(source.rstrip("/")) or source
    dest_label = f"{site}:{target}" if site else target
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
    local_files, listing = await asyncio.gather(
        loop.run_in_executor(None, scan_local_files, source),
        loop.run_in_executor(None, s3.list_objects, bucket, prefix),
    )
    if not local_files:
        progress.stop()
        raise CheckFailedError(
            f"reference {source} contains no files — refusing to check "
            "(wrong path or filesystem issue?)"
        )
    skip = len(prefix) + 1 if prefix else 0
    remote_sizes = {
        key[skip:]: size for key, size in listing if key[skip:]
    }
    report.total = len(local_files)
    progress.remove_task(walk_task_id)
    check_task_id = progress.add_task(
        f"[cyan]Checking {label}[/cyan]", total=len(local_files), unit="files",
    )

    try:
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
                await _check_one_s3(
                    bucket, source, prefix, rel, local_files[rel],
                    remote_sizes, report,
                    algo=algo, fix=fix, loop=loop,
                )
                report.checked += 1
                progress.update(check_task_id, completed=report.checked)
                await report.maybe_report()

        await asyncio.gather(
            _producer(), *[_worker() for _ in range(max_concurrent)],
        )

        # Extra S3 objects (the local copy is the reference).
        for rel in sorted(remote_sizes):
            if rel in local_files:
                continue
            key = f"{prefix}/{rel}" if prefix else rel
            fix_label = None
            detail = "not in the local reference"
            if delete_extra:
                # Only ever delete debris from an interrupted transfer;
                # any other extra object is kept and reported.
                if is_failed_transfer_leftover(rel, local_files):
                    await loop.run_in_executor(
                        None, lambda k=key: s3.delete(k, bucket=bucket),
                    )
                    fix_label = "deleted"
                else:
                    detail += " (kept: not a failed-transfer leftover)"
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
    """CLI entry point for ``nexus-transfers check-files-s3``."""
    parser = argparse.ArgumentParser(
        description="Verify an S3 copy against the local reference "
                    "(sizes by default, hashes with --hash), optionally "
                    "fixing it",
    )
    parser.add_argument("--source", required=True,
                        help="Local reference directory")
    parser.add_argument(
        "--target", required=True,
        help="S3 copy to verify: s3://bucket[/prefix]",
    )
    parser.add_argument(
        "--broker-url",
        default=cli_default("broker_url", "check_files_s3", default=None),
        help="Relay WebSocket URL for monitoring (default: none — monitoring disabled)",
    )
    parser.add_argument(
        "--name", default=cli_default("name", "check_files_s3", default=None),
        help="Client name on the relay (default: auto-generated)",
    )
    parser.add_argument(
        "--site", default=cli_default("site", "check_files_s3", default=None),
        help="Site label for monitor messages",
    )
    parser.add_argument(
        "--hash", dest="algo", metavar="ALGO",
        default=cli_default("hash", "check_files_s3", default=None),
        help="Hash algorithm (e.g. md5, sha256); streams every object back "
             "from S3, so every byte is re-downloaded "
             "(default: compare sizes only)",
    )
    parser.add_argument(
        "--fix", action="store_true",
        default=cli_default("fix", "check_files_s3", default=False),
        help="Re-upload corrupt or missing objects instead of failing",
    )
    parser.add_argument(
        "--delete-extra", action="store_true",
        default=cli_default("delete_extra", "check_files_s3", default=False),
        help="Delete extra objects left over by an interrupted transfer "
             "(<base>.<hex>[.tmp] with <base> in the local reference); other "
             "extras are only reported, never deleted",
    )
    parser.add_argument(
        "--max-concurrent", type=int,
        default=cli_default("max_concurrent", "check_files_s3", default=8,
                            type_fn=int),
        help="Maximum parallel file checks (default: 8)",
    )
    parser.add_argument(
        "--no-verify", action="store_true",
        default=cli_default("no_verify", "check_files_s3", default=False),
        help="Skip TLS verification for the relay connection",
    )
    parser.add_argument(
        "--debug", action="store_true",
        default=cli_default("debug", "check_files_s3", default=False),
        help="Enable debug logging",
    )
    args = parser.parse_args()

    setup_cli_logging(debug=args.debug)

    tag = args.site or "check-s3"
    name = args.name or f"{tag}-{uuid.uuid4().hex[:8]}"

    try:
        report = asyncio.run(
            _check_s3(
                source=args.source,
                target=args.target,
                broker_url=args.broker_url,
                name=name,
                site=args.site,
                algo=args.algo,
                fix=args.fix,
                delete_extra=args.delete_extra,
                max_concurrent=args.max_concurrent,
                ssl_verify=not args.no_verify,
            )
        )
    except CheckFailedError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(2)
    if not report.ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
