"""Recursive remote-to-local directory copy orchestrator."""

import asyncio
import logging
import os

from nexus_transfers._progress import _fmt_binary
from ._errors import PeerNotFoundError
from ._io import _move_file_atomic, _write_file

_logger = logging.getLogger(__name__)


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
        queue: asyncio.Queue = asyncio.Queue()

        async def _walk_and_enqueue():
            await self._walk_remote(
                self._remote_path, self._local_path, queue,
                walk_task=walk_task,
            )
            for _ in range(self._max_concurrent):
                await queue.put(None)

        sem = asyncio.Semaphore(self._max_concurrent)

        async def _worker():
            while True:
                item = await queue.get()
                if item is None:
                    return
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
                    continue

                async with sem:
                    data = await self._transfer_file(remote_file)
                    file_size = await self._save_file(data, local_file)
                    self._total_bytes += file_size
                    self._done_count += 1
                    progress.update(
                        copy_task,
                        completed=self._done_count + self._skipped,
                    )
                    await self._maybe_report_progress()

        try:
            await asyncio.gather(
                _walk_and_enqueue(),
                *[_worker() for _ in range(self._max_concurrent)],
            )
        finally:
            progress.remove_task(walk_task)
            progress.remove_task(copy_task)
        await self._print_summary()

    # -- remote walk -------------------------------------------------------

    async def _walk_remote(self, remote_path, local_path, queue,
                           walk_task=None, _counter=None):
        """Recursively walk and enqueue files as they are discovered."""
        if _counter is None:
            _counter = [0]
        os.makedirs(local_path, exist_ok=True)
        offset = 0
        limit = 1000
        while True:
            page = await self._list_dir_with_retry(remote_path, offset=offset)
            dirs = []
            for entry in page:
                name = entry["name"]
                remote_child = (
                    f"{remote_path}/{name}" if remote_path != "." else name
                )
                local_child = os.path.join(local_path, name)
                if entry["type"] == "dir":
                    dirs.append((remote_child, local_child))
                else:
                    _counter[0] += 1
                    if walk_task is not None:
                        self._client._progress.update(
                            walk_task, completed=_counter[0],
                        )
                    await queue.put(
                        (remote_child, local_child, entry.get("size"))
                    )
            if len(page) < limit:
                break
            offset += len(page)

        for remote_child, local_child in dirs:
            await self._walk_remote(
                remote_child, local_child, queue,
                walk_task=walk_task, _counter=_counter,
            )

    async def _list_dir_with_retry(self, remote_path, offset=0, limit=1000):
        """Fetch a page of directory entries, retrying on transient errors."""
        while True:
            try:
                return await self._client.send(
                    f"{self._target}.list_dir", remote_path,
                    include_size=self._track_bytes,
                    offset=offset, limit=limit,
                )
            except (PeerNotFoundError, ConnectionError,
                    asyncio.TimeoutError) as exc:
                _logger.warning(
                    "Listing %s failed (%s), retrying in %.1fs …",
                    remote_path, exc, self._client.peer_delay,
                )
                await self._client.monitor(
                    f"{self._client.name}: listing {remote_path} failed "
                    f"({type(exc).__name__}), retrying …",
                    status="warning",
                )
                await asyncio.sleep(self._client.peer_delay)

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

    async def _transfer_file(self, remote_file):
        """Download a single file, retrying on transient errors."""
        while True:
            try:
                if self._use_s3:
                    return await self._client.send(
                        f"{self._target}.get_file", remote_file,
                        use_s3=True, s3_prefix=self._s3_prefix,
                    )
                else:
                    return await self._client.send(
                        f"{self._target}.get_file", remote_file,
                        chunk_size=self._chunk_size,
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
                None, _move_file_atomic, data, local_file,
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
