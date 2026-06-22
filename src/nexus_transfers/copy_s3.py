"""CLI tools: copy a local file or directory to/from an S3 bucket.

Usage::

    nexus-transfers copy-to-s3 --source /data/dataset.zarr --target s3://bucket/datasets/dataset.zarr
    nexus-transfers copy-from-s3 --source s3://bucket/datasets/dataset.zarr --target /data/dataset.zarr

Credentials and endpoint come from the ``NEXUS_TRANSFER_S3_*`` environment
variables (or the config file); the ``s3://bucket/...`` argument overrides
only the bucket name. Already-present files with a matching size are
skipped, so an interrupted copy can be resumed by re-running the command.
"""

import argparse
import asyncio
import logging
import os
import sys
import threading
import uuid
from pathlib import PurePosixPath
from typing import Callable

from rich.console import Console
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
    _fmt_binary,
    setup_cli_logging,
)
from nexus_transfers.client import Client
from nexus_transfers.config import cli_default
from nexus_transfers.copy_ssh import _list_local

_logger = logging.getLogger(__name__)

# Each item is (local_path, s3_key, size) regardless of direction.
_Item = tuple[str, str, int]


def _key_for(prefix: str | None, rel: str) -> str:
    """Return the object key for *rel* under *prefix* (POSIX separators)."""
    rel = PurePosixPath(rel).as_posix()
    return f"{prefix}/{rel}" if prefix else rel


async def _upload_items(
    source: str, target_url: str,
) -> tuple[str, list[_Item], int]:
    """Resolve the upload item list.

    Returns ``(bucket, items, total_size)`` where each item is
    ``(local_path, s3_key, size)``.

    Parameters
    ----------
    source : str
        Local file or directory.
    target_url : str
        Destination ``s3://bucket[/prefix]`` URL.
    """
    bucket, prefix = s3.parse_s3_url(target_url)
    if os.path.isfile(source):
        size = os.path.getsize(source)
        if prefix is None or target_url.rstrip().endswith("/"):
            key = _key_for(prefix, os.path.basename(source))
        else:
            key = prefix
        return bucket, [(source, key, size)], size
    if not os.path.isdir(source):
        raise FileNotFoundError(f"Source {source!r} does not exist")
    walked, _count, total_size = await _list_local(source)
    items = [
        (local_path, _key_for(prefix, rel), size)
        for local_path, rel, size in walked
    ]
    return bucket, items, total_size


async def _download_items(
    source_url: str, target: str,
) -> tuple[str, list[_Item], int]:
    """Resolve the download item list.

    Returns ``(bucket, items, total_size)`` where each item is
    ``(local_path, s3_key, size)``.

    Parameters
    ----------
    source_url : str
        Source ``s3://bucket/key-or-prefix`` URL.
    target : str
        Local destination file or directory.
    """
    bucket, prefix = s3.parse_s3_url(source_url)
    loop = asyncio.get_running_loop()
    if prefix is not None:
        # An object stored at exactly the given key wins over a prefix.
        size = await loop.run_in_executor(None, s3.head_object, bucket, prefix)
        if size is not None:
            if os.path.isdir(target) or target.endswith(os.sep):
                local = os.path.join(target, PurePosixPath(prefix).name)
            else:
                local = target
            return bucket, [(local, prefix, size)], size
    listing = await loop.run_in_executor(None, s3.list_objects, bucket, prefix)
    if not listing:
        raise FileNotFoundError(
            f"No object found at s3://{bucket}/{prefix or ''}"
        )
    items: list[_Item] = []
    skip = len(prefix) + 1 if prefix else 0
    for key, size in listing:
        rel = key[skip:]
        if not rel:
            continue  # directory-marker object at the prefix itself
        items.append((os.path.join(target, *rel.split("/")), key, size))
    total_size = sum(size for _, _, size in items)
    return bucket, items, total_size


async def _copy_s3(
    direction: str,
    source: str,
    target: str,
    broker_url: str | None,
    name: str,
    site: str | None,
    max_concurrent: int,
    track_bytes: bool,
    ssl_verify: bool,
    on_monitor: Callable | None = None,
    quiet: bool = False,
) -> None:
    """Copy between the local disk and S3 (shared body of both commands).

    Parameters
    ----------
    direction : str
        ``"up"`` (local -> S3) or ``"down"`` (S3 -> local).
    source : str
        Local path (up) or ``s3://`` URL (down).
    target : str
        ``s3://`` URL (up) or local path (down).
    broker_url : str or None
        Relay WebSocket URL for monitoring only; ``None`` disables monitoring.
    name : str
        Client name on the relay.
    site : str or None
        Site label for monitor messages.
    max_concurrent : int
        Number of parallel S3 transfers.
    track_bytes : bool
        Show byte-based progress instead of file count.
    ssl_verify : bool
        Verify TLS certificate for the relay connection.
    on_monitor : callable, optional
        Async callback invoked with ``(message, status=..., **kwargs)`` for
        every monitor event, mirroring :func:`nexus_transfers.copy.copy`.
    quiet : bool
        If True, suppress rich console output (monitor events still fire).
    """
    console = Console(quiet=quiet)
    loop = asyncio.get_running_loop()

    if direction == "up":
        source = os.path.expanduser(source)
        label = os.path.basename(source.rstrip("/")) or source
        dest_label = target
    else:
        target = os.path.expanduser(target)
        label = PurePosixPath(s3.parse_s3_url(source)[1] or "bucket").name
        dest_label = target
    if not quiet:
        console.print(
            f"Copying [yellow]{source}[/yellow] -> [yellow]{target}[/yellow]"
        )

    monitor_client: Client | None = None
    if broker_url:
        try:
            # Monitoring is best-effort: keep retrying forever so a dropped
            # relay connection silently reconnects instead of permanently
            # losing live progress for the rest of the copy.
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
        f"{name}: starting copy {source} -> {target}",
        status="progress",
    )

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

    start = loop.time()
    total_bytes = 0
    done_count = 0
    skipped = 0
    skipped_bytes = 0
    lock = threading.Lock()

    unit = "bytes" if track_bytes else "files"
    walk_task_id = progress.add_task(
        f"[magenta]Listing {label}[/magenta]", total=None, unit="files",
    )
    copy_task_id = progress.add_task(
        f"[cyan]Copying {label}[/cyan]", total=None, unit=unit,
    )
    progress.start()

    try:
        if direction == "up":
            bucket, items, total_size = await _upload_items(source, target)
            remote = dict(await loop.run_in_executor(
                None, s3.list_objects, bucket, s3.parse_s3_url(target)[1],
            ))
        else:
            bucket, items, total_size = await _download_items(source, target)
            remote = {}
        progress.update(
            copy_task_id, total=total_size if track_bytes else len(items),
        )
        progress.remove_task(walk_task_id)

        def _advance(is_skip: bool, n_bytes: int, n_files: int) -> None:
            """Update shared counters and the rich bar (thread-safe)."""
            nonlocal total_bytes, done_count, skipped, skipped_bytes
            with lock:
                if is_skip:
                    skipped += n_files
                    skipped_bytes += n_bytes
                else:
                    total_bytes += n_bytes
                    done_count += n_files
                if track_bytes:
                    progress.advance(copy_task_id, n_bytes)
                else:
                    progress.update(copy_task_id, completed=done_count + skipped)
                if skipped:
                    progress.update(
                        copy_task_id,
                        description=(
                            f"[cyan]Copying {label}[/cyan] "
                            f"[dim]({skipped} skipped)[/dim]"
                        ),
                    )

        def _payload() -> tuple[float, dict]:
            with lock:
                elapsed = loop.time() - start
                rate = total_bytes / elapsed if elapsed > 0 else 0
                return rate, {
                    "label": f"{name}: {done_count} files",
                    "value": total_bytes + skipped_bytes,
                    "maximum": total_size or None,
                    "unit": "byte",
                    "total_transferred": total_bytes + skipped_bytes,
                    "files_done": done_count,
                    "files_skipped": skipped,
                    "rate": rate,
                }

        async def _ticker(stop_event: asyncio.Event) -> None:
            """Emit aggregated progress every 30s until *stop_event* is set."""
            while True:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=30)
                    return
                except asyncio.TimeoutError:
                    pass
                rate, payload = _payload()
                skip_suffix = (
                    f" [{skipped} skipped, {_fmt_binary(skipped_bytes)}]"
                    if skipped else ""
                )
                await _emit(
                    f"{name}: {done_count} files "
                    f"({_fmt_binary(total_bytes)}, {_fmt_binary(rate)}/s)"
                    + skip_suffix,
                    status="progress",
                    progress=payload,
                )

        def _needs_transfer(item: _Item) -> bool:
            """Size-based resume check (runs in an executor thread)."""
            local_path, key, size = item
            if direction == "up":
                return remote.get(key) != size
            try:
                return os.path.getsize(local_path) != size
            except OSError:
                return True

        def _transfer(item: _Item) -> None:
            """Move one file (runs in an executor thread)."""
            local_path, key, size = item
            if direction == "up":
                s3.upload_file(local_path, s3_key=key, bucket=bucket)
            else:
                tmp = s3.download_file(key, target_path=local_path, bucket=bucket)
                os.replace(tmp, local_path)

        queue: asyncio.Queue = asyncio.Queue(maxsize=max_concurrent * 4)

        async def _producer() -> None:
            for item in items:
                if await loop.run_in_executor(None, _needs_transfer, item):
                    await queue.put(item)
                else:
                    _logger.debug("Skipping %s (size matches)", item[1])
                    _advance(True, item[2], 1)
            for _ in range(max_concurrent):
                await queue.put(None)

        async def _worker() -> None:
            while True:
                item = await queue.get()
                if item is None:
                    return
                await loop.run_in_executor(None, _transfer, item)
                _advance(False, item[2], 1)

        stop_event = asyncio.Event()
        ticker = asyncio.create_task(_ticker(stop_event))
        try:
            await asyncio.gather(
                _producer(), *[_worker() for _ in range(max_concurrent)],
            )
        finally:
            stop_event.set()
            await ticker
    finally:
        progress.stop()

    if skipped:
        _logger.info(
            "Skipped %d already-complete file(s) (%s)",
            skipped, _fmt_binary(skipped_bytes),
        )
        if not quiet:
            console.print(
                f"Skipped [bold]{skipped}[/bold] already-complete file(s) "
                f"([bold]{_fmt_binary(skipped_bytes)}[/bold])"
            )

    elapsed = loop.time() - start
    rate = total_bytes / elapsed if elapsed > 0 else 0
    skip_suffix = (
        f" [{skipped} skipped, {_fmt_binary(skipped_bytes)}]"
        if skipped else ""
    )
    summary = (
        f"Transferred {_fmt_binary(total_bytes)} "
        f"in {elapsed:.1f}s ({_fmt_binary(rate)}/s)" + skip_suffix
    )
    if not quiet:
        console.print(
            f"Transferred [bold]{_fmt_binary(total_bytes)}[/bold] "
            f"in [bold]{elapsed:.1f}s[/bold] "
            f"([bold]{_fmt_binary(rate)}/s[/bold])"
        )

    await _emit(
        f"{name}: {summary}",
        status="ok",
        progress=_payload()[1],
    )

    if monitor_client:
        await monitor_client.close()


async def copy_to_s3(
    source: str,
    target: str,
    *,
    broker_url: str | None = None,
    name: str | None = None,
    site: str | None = None,
    max_concurrent: int = 8,
    track_bytes: bool = False,
    ssl_verify: bool = True,
    on_monitor: Callable | None = None,
    quiet: bool = False,
) -> None:
    """Copy the local *source* file or directory to the S3 *target*.

    Parameters
    ----------
    source : str
        Local file or directory to copy.
    target : str
        Destination ``s3://bucket[/prefix]`` URL.
    broker_url : str or None
        Relay WebSocket URL for monitoring only; ``None`` disables monitoring.
    name : str or None
        Client name on the relay (auto-generated when None).
    site : str or None
        Site label for monitor messages.
    max_concurrent : int
        Number of parallel S3 uploads.
    track_bytes : bool
        Show byte-based progress instead of file count.
    ssl_verify : bool
        Verify TLS certificate for the relay connection.
    on_monitor : callable, optional
        Async callback invoked for every monitor event.
    quiet : bool
        If True, suppress rich console output (monitor events still fire).
    """
    await _copy_s3(
        "up", source, target, broker_url,
        name or f"{site or 's3-copy'}-{uuid.uuid4().hex[:8]}", site,
        max_concurrent, track_bytes, ssl_verify, on_monitor, quiet,
    )


async def copy_from_s3(
    source: str,
    target: str,
    *,
    broker_url: str | None = None,
    name: str | None = None,
    site: str | None = None,
    max_concurrent: int = 8,
    track_bytes: bool = False,
    ssl_verify: bool = True,
    on_monitor: Callable | None = None,
    quiet: bool = False,
) -> None:
    """Copy the S3 *source* object or prefix to the local *target*.

    Parameters
    ----------
    source : str
        Source ``s3://bucket/key-or-prefix`` URL.
    target : str
        Local destination file or directory.
    broker_url : str or None
        Relay WebSocket URL for monitoring only; ``None`` disables monitoring.
    name : str or None
        Client name on the relay (auto-generated when None).
    site : str or None
        Site label for monitor messages.
    max_concurrent : int
        Number of parallel S3 downloads.
    track_bytes : bool
        Show byte-based progress instead of file count.
    ssl_verify : bool
        Verify TLS certificate for the relay connection.
    on_monitor : callable, optional
        Async callback invoked for every monitor event.
    quiet : bool
        If True, suppress rich console output (monitor events still fire).
    """
    await _copy_s3(
        "down", source, target, broker_url,
        name or f"{site or 's3-copy'}-{uuid.uuid4().hex[:8]}", site,
        max_concurrent, track_bytes, ssl_verify, on_monitor, quiet,
    )


def _main(direction: str) -> None:
    """Shared argparse entry point for both commands."""
    if direction == "up":
        description = "Copy a local file or directory to an S3 bucket"
        source_help = "Local file or directory to copy"
        target_help = "Destination: s3://bucket[/prefix]"
    else:
        description = "Copy an S3 object or prefix to the local disk"
        source_help = "Source: s3://bucket/key-or-prefix"
        target_help = "Local destination file or directory"

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--source", required=True, help=source_help)
    parser.add_argument("--target", required=True, help=target_help)
    parser.add_argument(
        "--broker-url",
        default=cli_default("broker_url", "copy_s3", default=None),
        help="Relay WebSocket URL for monitoring (default: none — monitoring disabled)",
    )
    parser.add_argument(
        "--name", default=cli_default("name", "copy_s3", default=None),
        help="Client name on the relay (default: auto-generated)",
    )
    parser.add_argument(
        "--site", default=cli_default("site", "copy_s3", default=None),
        help="Site label for monitor messages",
    )
    parser.add_argument(
        "--max-concurrent", type=int,
        default=cli_default("max_concurrent", "copy_s3", default=8, type_fn=int),
        help="Number of parallel S3 transfers (default: 8)",
    )
    parser.add_argument(
        "--size", action="store_true",
        default=cli_default("size", "copy_s3", default=False),
        help="Show byte-based progress instead of file count",
    )
    parser.add_argument(
        "--quiet", action="store_true",
        default=cli_default("quiet", "copy_s3", default=False),
        help="Suppress console output (monitor events still fire)",
    )
    parser.add_argument(
        "--no-verify", action="store_true",
        default=cli_default("no_verify", "copy_s3", default=False),
        help="Skip TLS verification for the relay connection",
    )
    parser.add_argument(
        "--debug", action="store_true",
        default=cli_default("debug", "copy_s3", default=False),
        help="Enable debug logging",
    )
    args = parser.parse_args()

    setup_cli_logging(debug=args.debug)

    fn = copy_to_s3 if direction == "up" else copy_from_s3
    try:
        asyncio.run(
            fn(
                args.source,
                args.target,
                broker_url=args.broker_url,
                name=args.name,
                site=args.site,
                max_concurrent=args.max_concurrent,
                track_bytes=args.size,
                ssl_verify=not args.no_verify,
                quiet=args.quiet,
            )
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)


def main_to() -> None:
    """CLI entry point for ``nexus-transfers copy-to-s3``."""
    _main("up")


def main_from() -> None:
    """CLI entry point for ``nexus-transfers copy-from-s3``."""
    _main("down")


if __name__ == "__main__":
    main_to()
