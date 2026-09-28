"""CLI tools: copy a local file or directory to/from an S3 bucket.

Usage::

    nexus-transfers copy-to-s3 --source /data/dataset.zarr --target s3://bucket/datasets/dataset.zarr
    nexus-transfers copy-from-s3 --source s3://bucket/datasets/dataset.zarr --target /data/dataset.zarr

Credentials and endpoint come from the ``NEXUS_TRANSFERS_S3_*`` environment
variables (or the config file); the ``s3://bucket/...`` argument overrides
only the bucket name. Already-present files with a matching size are
skipped, so an interrupted copy can be resumed by re-running the command.
"""

import asyncio
import logging
import os
import sys
import uuid
from pathlib import PurePosixPath
from typing import Callable

from nexus_transfers import s3
from nexus_transfers._progress import (
    CopyStats,
    make_console,
    make_progress,
    setup_cli_logging,
)
from nexus_transfers._run import Monitor, run_workers, ticking
from nexus_transfers._cli import CommandParser

_logger = logging.getLogger(__name__)

# Each item is (local_path, s3_key, size) regardless of direction.
_Item = tuple[str, str, int]

# Number of files per listing batch. S3 LISTs page 1000 keys at a time, so this
# matches the natural object-store pagination unit; the local walk is chunked to
# the same size for symmetry.
_BATCH_SIZE = 1000

# Default depth of the resume scan (target existence/size checks). These are
# latency-bound HEAD/stat calls, so they run far deeper than the bandwidth-bound
# transfers, mirroring ``copy_ssh``'s ``--stat-concurrency``.
DEFAULT_STAT_CONCURRENCY = 64


def _key_for(prefix: str | None, rel: str) -> str:
    """Return the object key for *rel* under *prefix* (POSIX separators)."""
    rel = PurePosixPath(rel).as_posix()
    return f"{prefix}/{rel}" if prefix else rel


def _scandir_split(dirpath: str, rel_prefix: str):
    """Scan one directory, returning ``(subdirs, files)``.

    ``subdirs`` is a list of ``(path, relative_path)`` for recursion; ``files``
    is a list of ``(local_path, relative_path, size)``. Runs in an executor
    thread so the (potentially NFS) ``stat`` calls do not block the event loop.
    """
    subdirs: list[tuple[str, str]] = []
    files: list[tuple[str, str, int]] = []
    for entry in os.scandir(dirpath):
        rel = os.path.join(rel_prefix, entry.name) if rel_prefix else entry.name
        if entry.is_dir(follow_symlinks=False):
            subdirs.append((entry.path, rel))
        else:
            try:
                size = entry.stat().st_size
            except OSError:
                size = 0
            files.append((entry.path, rel, size))
    return subdirs, files


async def _iter_upload_batches(source: str, target_url: str):
    """Yield batches of ``(local_path, s3_key, size)`` items for an upload.

    A single file yields a one-item batch; a directory is walked one level at a
    time and accumulated into batches of up to :data:`_BATCH_SIZE` so transfers
    can begin before the whole tree has been enumerated.

    Parameters
    ----------
    source : str
        Local file or directory.
    target_url : str
        Destination ``s3://bucket[/prefix]`` URL.
    """
    loop = asyncio.get_running_loop()
    prefix = s3.parse_s3_url(target_url)[1]
    if os.path.isfile(source):
        size = os.path.getsize(source)
        if prefix is None or target_url.rstrip().endswith("/"):
            key = _key_for(prefix, os.path.basename(source))
        else:
            key = prefix
        yield [(source, key, size)]
        return
    if not os.path.isdir(source):
        raise FileNotFoundError(f"Source {source!r} does not exist")
    stack = [(source, "")]
    batch: list[_Item] = []
    while stack:
        dirpath, rel_prefix = stack.pop()
        subdirs, files = await loop.run_in_executor(
            None, _scandir_split, dirpath, rel_prefix,
        )
        stack.extend(subdirs)
        for local_path, rel, size in files:
            batch.append((local_path, _key_for(prefix, rel), size))
            if len(batch) >= _BATCH_SIZE:
                yield batch
                batch = []
    if batch:
        yield batch


async def _iter_download_batches(source_url: str, target: str):
    """Yield batches of ``(local_path, s3_key, size)`` items for a download.

    An object stored at exactly the given key wins over a prefix and yields a
    single-item batch; otherwise the prefix is listed page by page (S3 pages
    1000 keys at a time) so downloads can begin before the whole listing is
    retrieved. Raises ``FileNotFoundError`` if nothing matches.

    Parameters
    ----------
    source_url : str
        Source ``s3://bucket/key-or-prefix`` URL.
    target : str
        Local destination file or directory.
    """
    loop = asyncio.get_running_loop()
    bucket, prefix = s3.parse_s3_url(source_url)
    if prefix is not None:
        # An object stored at exactly the given key wins over a prefix.
        size = await loop.run_in_executor(None, s3.head_object, bucket, prefix)
        if size is not None:
            if os.path.isdir(target) or target.endswith(os.sep):
                local = os.path.join(target, PurePosixPath(prefix).name)
            else:
                local = target
            yield [(local, prefix, size)]
            return
    skip = len(prefix) + 1 if prefix else 0
    found = False
    pages = s3.list_object_batches(bucket, prefix)
    while True:
        page = await loop.run_in_executor(None, s3.next_batch, pages)
        if page is None:
            break
        batch: list[_Item] = []
        for meta in page:
            key, size = meta["path"], meta["size"]
            rel = key[skip:]
            if not rel:
                continue  # directory-marker object at the prefix itself
            batch.append((os.path.join(target, *rel.split("/")), key, size))
        if batch:
            found = True
            yield batch
    if not found:
        raise FileNotFoundError(
            f"No object found at s3://{bucket}/{prefix or ''}"
        )


async def _copy_s3(
    direction: str,
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
    steal: bool = False,
    stat_concurrency: int = DEFAULT_STAT_CONCURRENCY,
) -> None:
    """Copy between the local disk and S3: the body of :func:`copy_to_s3`
    and :func:`copy_from_s3`.

    Parameters
    ----------
    direction : str
        ``"up"`` (local -> S3) or ``"down"`` (S3 -> local).
    source : str
        Local file or directory (up) or ``s3://bucket/key-or-prefix`` (down).
    target : str
        ``s3://bucket[/prefix]`` (up) or local file or directory (down).
    broker_url : str or None
        Relay WebSocket URL for monitoring only; ``None`` disables monitoring.
    name : str or None
        Client name on the relay (default: ``<site or s3-copy>-<8hex>``).
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
    steal : bool
        If True, kill any client already registered under ``name`` (soft kill,
        then hard kill) and take over the name before connecting.  A no-op
        without ``broker_url``.  Key ``name`` on the unit of work
        (``nexus-location-<location_uuid>``) for a per-transfer interlock.
    stat_concurrency : int
        Depth of the resume scan: the maximum number of concurrent target
        existence/size checks (S3 ``HEAD`` for uploads, local ``stat`` for
        downloads). Runs far deeper than ``max_concurrent`` so latency-bound
        checks overlap with the bandwidth-bound transfers.
    """
    name = name or f"{site or 's3-copy'}-{uuid.uuid4().hex[:8]}"
    console = make_console(quiet=quiet)
    loop = asyncio.get_running_loop()

    if direction == "up":
        source = os.path.expanduser(source)
        label = os.path.basename(source.rstrip("/")) or source
        bucket = s3.parse_s3_url(target)[0]
    else:
        target = os.path.expanduser(target)
        bucket, key = s3.parse_s3_url(source)
        label = PurePosixPath(key).name if key else bucket
    console.print(
        f"Copying [yellow]{source}[/yellow] -> [yellow]{target}[/yellow]"
    )

    def _needs_transfer(item: _Item) -> bool:
        """Resume check against the target (runs in an executor thread).

        For uploads this ``HEAD``s the destination object; for downloads it
        ``stat``s the local file. A size mismatch (or a missing target)
        means the file must be transferred.
        """
        local_path, key, size = item
        if direction == "up":
            return s3.head_object(bucket, key) != size
        try:
            return os.path.getsize(local_path) != size
        except OSError:
            return True

    def _transfer(item: _Item) -> None:
        """Move one file (runs in an executor thread).

        Uploads put the object straight at its final key (S3 ``PUT`` is
        atomic). Downloads stream to a ``<name>.<hex>.tmp`` file next to
        the destination and ``os.replace`` it into place, so a partial
        download never appears as a complete file.
        """
        local_path, key, size = item
        if direction == "up":
            s3.upload_file(local_path, s3_key=key, bucket=bucket)
        else:
            tmp = s3.download_file(key, target_path=local_path, bucket=bucket)
            os.replace(tmp, local_path)

    async with await Monitor.connect(
        name, broker_url, ssl_verify=ssl_verify, steal=steal,
        on_monitor=on_monitor,
    ) as monitor:
        await monitor.emit(
            f"{name}: starting copy {source} -> {target}", status="progress",
        )

        progress = make_progress(quiet)
        stats = CopyStats(name, label, progress, track_bytes=track_bytes)
        progress.start()

        # Two-stage pipeline, mirroring copy_ssh: the source is listed in
        # batches of up to _BATCH_SIZE into a small batch queue; a classifier
        # checks each batch against the target at stat_concurrency depth and
        # hands the files that need transfer to max_concurrent workers.
        stat_sem = asyncio.Semaphore(stat_concurrency)

        async def _batches():
            if direction == "up":
                batches = _iter_upload_batches(source, target)
            else:
                batches = _iter_download_batches(source, target)
            async for batch in batches:
                stats.add_total(len(batch), sum(size for *_, size in batch))
                progress.update(stats.walk_task, advance=len(batch))
                yield batch
            stats.listed()

        async def _classify(item: _Item, put) -> None:
            async with stat_sem:
                needs = await loop.run_in_executor(None, _needs_transfer, item)
            if needs:
                await put(item)
            else:
                _logger.debug("Skipping %s (size matches)", item[1])
                stats.advance(True, item[2])

        async def _list_and_classify(put) -> None:
            async def _classify_batch(batch: list[_Item]) -> None:
                await asyncio.gather(*[_classify(it, put) for it in batch])

            await run_workers(_batches(), _classify_batch, 1, maxsize=2)

        async def _transfer_one(item: _Item) -> None:
            await loop.run_in_executor(None, _transfer, item)
            stats.advance(False, item[2])

        try:
            async with ticking(30, lambda: stats.heartbeat(monitor)):
                # Immediate heartbeat entering the listing phase so the
                # catalogue is refreshed before a (potentially long) listing,
                # not only once the first 30s tick fires.
                await monitor.emit(
                    f"{name}: listing {label}", status="progress",
                    progress=stats.payload(),
                )
                await run_workers(
                    _list_and_classify, _transfer_one, max_concurrent,
                    maxsize=max_concurrent * 4,
                )
        finally:
            progress.stop()

        await stats.finish(monitor, console)


async def copy_to_s3(source: str, target: str, **kwargs) -> None:
    """Copy the local *source* file or directory to the S3 *target*
    ``s3://bucket[/prefix]``, resumably (objects already there with the same
    size are skipped).

    Keyword arguments: those of :func:`_copy_s3`.
    """
    await _copy_s3("up", source, target, **kwargs)


async def copy_from_s3(source: str, target: str, **kwargs) -> None:
    """Copy the S3 *source* object or prefix ``s3://bucket/key-or-prefix`` to
    the local *target* file or directory, resumably (local files already
    there with the same size are skipped).

    Keyword arguments: those of :func:`_copy_s3`.
    """
    await _copy_s3("down", source, target, **kwargs)


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

    parser = CommandParser("copy_s3", description=description)
    parser.add_argument("--source", required=True, help=source_help)
    parser.add_argument("--target", required=True, help=target_help)
    parser.monitor_options()
    parser.option("--max-concurrent", type=int, default=8,
                  help="Number of parallel S3 transfers (default: 8)")
    parser.option("--stat-concurrency", type=int,
                  default=DEFAULT_STAT_CONCURRENCY,
                  help="Max concurrent target existence/size checks during "
                       f"the resume scan (default: {DEFAULT_STAT_CONCURRENCY})")
    parser.option("--size", action="store_true",
                  help="Show byte-based progress instead of file count")
    parser.option("--quiet", action="store_true",
                  help="Suppress console output (monitor events still fire)")
    parser.option(
        "--steal", action="store_true",
        help="If a client is already registered under --name, kill it (soft "
             "kill first, then hard kill if it does not exit) and take over "
             "the name. With a name keyed on the transfer (e.g. "
             "nexus-location-<location_uuid>) this guarantees only one S3 "
             "copy of it runs at a time. Requires --broker-url.",
    )
    parser.debug_option()
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
                steal=args.steal,
                stat_concurrency=args.stat_concurrency,
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
