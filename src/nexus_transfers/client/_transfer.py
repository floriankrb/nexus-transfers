"""Recursive remote-to-local directory copy orchestrator."""

import asyncio
import logging
import os

from nexus_transfers._progress import _fmt_binary
from nexus_transfers._run import run_workers
from ._errors import PeerNotFoundError
from ._io import _write_file

_logger = logging.getLogger(__name__)


_PAGE_SIZE = 1000


async def walk_peer_dir(client, target, remote_path, local_path, *,
                        include_size=False, make_dirs=False):
    """Walk the directory *remote_path* of the peer *target* with paged
    ``list_dir`` calls, yielding ``(remote_file, local_file, rel, size)``
    for every file: *local_file* mirrors it under *local_path*, *rel* is its
    POSIX path relative to *remote_path*, *size* is None unless
    *include_size*.

    Listing retries forever on :class:`PeerNotFoundError`,
    :class:`ConnectionError` and :class:`asyncio.TimeoutError`, sleeping
    ``client.peer_delay`` and emitting a ``warning`` monitor event.  With
    *make_dirs*, the local directories are created along the way.
    """

    async def _page(path, offset):
        while True:
            try:
                return await client.send(
                    f"{target}.list_dir", path, include_size=include_size,
                    offset=offset, limit=_PAGE_SIZE,
                )
            except (PeerNotFoundError, ConnectionError,
                    asyncio.TimeoutError) as exc:
                _logger.warning(
                    "Listing %s failed (%s), retrying in %.1fs …",
                    path, exc, client.peer_delay,
                )
                await client.monitor(
                    f"{client.name}: listing {path} failed "
                    f"({type(exc).__name__}), retrying …",
                    status="warning",
                )
                await asyncio.sleep(client.peer_delay)

    async def _walk(path, local, rel_prefix):
        if make_dirs:
            os.makedirs(local, exist_ok=True)
        # Recurse only once the whole directory is paged in: a subdirectory
        # can appear in any page, including a non-final one.
        dirs = []
        offset = 0
        while True:
            page = await _page(path, offset)
            for entry in page:
                name = entry["name"]
                child = f"{path}/{name}" if path != "." else name
                local_child = os.path.join(local, name)
                rel = f"{rel_prefix}/{name}" if rel_prefix else name
                if entry["type"] == "dir":
                    dirs.append((child, local_child, rel))
                else:
                    yield child, local_child, rel, entry.get("size")
            if len(page) < _PAGE_SIZE:
                break
            offset += len(page)
        for child, local_child, rel in dirs:
            async for item in _walk(child, local_child, rel):
                yield item

    async for item in _walk(remote_path, local_path, ""):
        yield item


class _DirectoryTransfer:
    """Orchestrates a recursive remote-to-local directory copy.

    Encapsulates walk, skip-detection, file download, progress tracking
    and monitor reporting.

    Parameters
    ----------
    client:
        The :class:`Client` instance that provides ``send`` and ``monitor``.
    target:
        Name of the remote nexus client.
    remote_path:
        Path on the remote client to copy from.
    local_path:
        Local destination directory.
    max_concurrent:
        Maximum number of parallel file transfers.
    chunk_size:
        Binary chunk size in bytes (relay mode only).
    use_s3:
        If True, stage transfers through S3.
    s3_prefix:
        Optional S3 key prefix for this batch.
    track_bytes:
        If True, verify resume skips by size and show byte progress.
    """

    _MONITOR_INTERVAL = 30  # seconds between progress reports

    def __init__(self, client, target, remote_path, local_path,
                 max_concurrent, chunk_size, use_s3, s3_prefix, track_bytes):
        self._client = client
        self._target = target
        self._remote_path = remote_path
        self._local_path = local_path
        self._max_concurrent = max_concurrent
        self._chunk_size = chunk_size
        self._use_s3 = use_s3
        self._s3_prefix = s3_prefix
        self._track_bytes = track_bytes

        self._label = os.path.basename(remote_path.rstrip("/")) or remote_path

        # Counters
        self._total_bytes = 0
        self._done_count = 0
        self._skipped = 0
        self._skipped_bytes = 0

        # Timing
        self._loop = asyncio.get_running_loop()
        self._start = self._loop.time()
        self._last_monitor_time = self._start

    # -- public entry point ------------------------------------------------

    async def run(self):
        """Walk the remote directory and transfer all files."""
        progress = self._client._progress

        walk_task = progress.add_task(
            f"[magenta]Listing {self._label}[/magenta]",
            total=None, unit="files",
        )
        copy_task = progress.add_task(
            f"[cyan]Copying {self._label}[/cyan]",
            total=None, unit="files",
        )
        async def _walk(put):
            nonlocal walk_task
            n = 0
            async for remote_file, local_file, _, size in walk_peer_dir(
                self._client, self._target, self._remote_path,
                self._local_path, include_size=self._track_bytes,
                make_dirs=True,
            ):
                n += 1
                progress.update(walk_task, completed=n)
                await put((remote_file, local_file, size))
            progress.remove_task(walk_task)
            walk_task = None

        async def _copy(item):
            remote_file, local_file, remote_size = item

            if self._should_skip(local_file, remote_size):
                self._add_skip(local_file)
                progress.update(
                    copy_task,
                    completed=self._done_count + self._skipped,
                    description=(
                        f"[cyan]Copying {self._label}[/cyan] "
                        f"[dim]({self._skipped} skipped)[/dim]"
                    ),
                )
                return

            data = await self._transfer_file(remote_file, local_file)
            file_size = await self._save_file(data, local_file)
            self._total_bytes += file_size
            self._done_count += 1
            progress.update(
                copy_task,
                completed=self._done_count + self._skipped,
            )
            await self._maybe_report_progress()

        try:
            await run_workers(_walk, _copy, self._max_concurrent)
        finally:
            if walk_task is not None:
                progress.remove_task(walk_task)
            progress.remove_task(copy_task)
        await self._print_summary()

    # -- skip detection ----------------------------------------------------

    def _should_skip(self, local_file, remote_size):
        """Return True if *local_file* already exists and can be skipped."""
        if not os.path.isfile(local_file):
            return False
        if self._track_bytes and remote_size is not None:
            try:
                local_size = os.path.getsize(local_file)
            except OSError:
                local_size = -1
            if local_size != remote_size:
                _logger.warning(
                    "Local file %s has size %d but remote size is %d — "
                    "will re-download",
                    local_file, local_size, remote_size,
                )
                return False
        return True

    def _add_skip(self, local_file):
        """Record a skipped file in the counters."""
        self._skipped += 1
        self._skipped_bytes += os.path.getsize(local_file)
        if self._skipped == 1 or self._skipped % 1000 == 0:
            _logger.info(
                "Skipping already-downloaded files: %d so far", self._skipped,
            )

    # -- file transfer -----------------------------------------------------

    async def _transfer_file(self, remote_file, local_file):
        """Download a single file, retrying on transient errors."""
        while True:
            try:
                if self._use_s3:
                    return await self._client.send(
                        f"{self._target}.get_file", remote_file,
                        use_s3=True, s3_prefix=self._s3_prefix,
                        _local_target=local_file,
                    )
                else:
                    # use_s3=False must be sent explicitly: the remote
                    # get_file defaults to S3 staging.
                    return await self._client.send(
                        f"{self._target}.get_file", remote_file,
                        chunk_size=self._chunk_size, use_s3=False,
                    )
            except (PeerNotFoundError, ConnectionError,
                    asyncio.TimeoutError) as exc:
                _logger.warning(
                    "Transfer of %s failed (%s), retrying in %.1fs …",
                    os.path.basename(remote_file), exc,
                    self._client.peer_delay,
                )
                await self._client.monitor(
                    f"{self._client.name}: transfer of "
                    f"{os.path.basename(remote_file)} failed "
                    f"({type(exc).__name__}), retrying …",
                    status="warning",
                )
                await asyncio.sleep(self._client.peer_delay)

    async def _save_file(self, data, local_file):
        """Write *data* to *local_file* atomically, return file size."""
        os.makedirs(os.path.dirname(local_file), exist_ok=True)
        file_size = os.path.getsize(data) if isinstance(data, str) else len(data)
        if isinstance(data, str):
            await self._loop.run_in_executor(
                None, os.replace, data, local_file,
            )
        else:
            await self._loop.run_in_executor(
                None, _write_file, local_file, data,
            )
        return file_size

    # -- progress reporting ------------------------------------------------

    async def _maybe_report_progress(self):
        """Send a monitor event if enough time has elapsed."""
        now = self._loop.time()
        if now - self._last_monitor_time < self._MONITOR_INTERVAL:
            return
        self._last_monitor_time = now
        elapsed = now - self._start
        rate = self._total_bytes / elapsed if elapsed > 0 else 0
        await self._client.monitor(
            f"{self._client.name}: {self._done_count} files "
            f"({_fmt_binary(self._total_bytes)}, "
            f"{_fmt_binary(rate)}/s)",
            status="progress",
            progress={
                "label": f"{self._client.name}: {self._done_count} files",
                "value": self._total_bytes + self._skipped_bytes,
                "unit": "byte",
                "total_transferred": self._total_bytes + self._skipped_bytes,
                "files_done": self._done_count,
                "files_skipped": self._skipped,
                "rate": rate,
            },
        )

    async def _print_summary(self):
        """Log and monitor the final transfer summary."""
        elapsed = self._loop.time() - self._start
        rate = self._total_bytes / elapsed if elapsed > 0 else 0
        console = self._client._progress.console

        if self._skipped:
            _logger.info(
                "Skipped %d already-complete file(s) (%s)",
                self._skipped, _fmt_binary(self._skipped_bytes),
            )
            console.print(
                f"Skipped [bold]{self._skipped}[/bold] already-complete "
                f"file(s) ([bold]{_fmt_binary(self._skipped_bytes)}[/bold])"
            )

        summary = (
            f"Transferred {_fmt_binary(self._total_bytes)} "
            f"in {elapsed:.1f}s ({_fmt_binary(rate)}/s)"
        )
        console.print(
            f"Transferred [bold]{_fmt_binary(self._total_bytes)}[/bold] "
            f"in [bold]{elapsed:.1f}s[/bold] "
            f"([bold]{_fmt_binary(rate)}/s[/bold])"
        )
        await self._client.monitor(
            f"{self._client.name}: {summary}", status="ok",
        )
