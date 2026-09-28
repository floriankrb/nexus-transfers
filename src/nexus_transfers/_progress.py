"""Shared progress-bar and console helpers for the CLI tools."""

import logging
import sys
import threading
import time

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    ProgressColumn,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
)
from rich.text import Text


def make_console(quiet: bool = False) -> Console:
    """Build a console suitable for both terminals and log files.

    When stdout is not a terminal (output piped or redirected to a log
    file), soft wrap is enabled so long lines — dataset paths in
    particular — are never wrapped at an assumed terminal width.

    Parameters
    ----------
    quiet : bool
        If True, the console discards all output.
    """
    return Console(quiet=quiet, soft_wrap=not sys.stdout.isatty())


def setup_cli_logging(debug: bool = False) -> None:
    """Configure logging for a CLI run.

    Uses ``RichHandler`` on a terminal; plain unwrapped log lines when the
    output is piped or redirected to a file.

    Parameters
    ----------
    debug : bool
        If True, set the DEBUG level instead of INFO.
    """
    level = logging.DEBUG if debug else logging.INFO
    if sys.stdout.isatty():
        from rich.logging import RichHandler
        logging.basicConfig(level=level,
                            handlers=[RichHandler(rich_tracebacks=True)])
    else:
        logging.basicConfig(
            level=level,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )


def _fmt_binary(n: float) -> str:
    """Format a byte count using binary prefixes (KiB, MiB, GiB, TiB, PiB)."""
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if n < 1024:
            return f"{n:.2f} {unit}"
        n /= 1024
    return f"{n:.2f} PiB"


class _CountOrBytesColumn(ProgressColumn):
    """Shows 'X / N files' when task has unit='files', otherwise 'X.x MiB / Y.y MiB'.

    When the total is unknown (still being discovered), shows just 'X files'.
    """

    def render(self, task) -> Text:
        completed = int(task.completed)
        if task.fields.get("unit") == "files":
            if task.total is None:
                return Text(f"{completed} files")
            return Text(f"{completed} / {int(task.total)} files")
        total = int(task.total) if task.total is not None else 0
        return Text(f"{_fmt_binary(completed)} / {_fmt_binary(total)}")


class _BinarySpeedColumn(ProgressColumn):
    """Renders transfer speed; files/s for unit='files' tasks, binary bytes/s otherwise."""

    def render(self, task) -> Text:
        speed = task.finished_speed or task.speed
        if speed is None:
            return Text("? /s", style="progress.data.speed")
        if task.fields.get("unit") == "files":
            return Text(f"{speed:.1f} files/s", style="progress.data.speed")
        return Text(_fmt_binary(speed) + "/s", style="progress.data.speed")


def make_progress(quiet: bool = False) -> Progress:
    """Build the transient progress display shared by the copy and check
    commands (hidden when *quiet*)."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        _CountOrBytesColumn(),
        _BinarySpeedColumn(),
        TimeRemainingColumn(),
        transient=True,
        disable=quiet,
    )


class CopyStats:
    """Thread-safe counters of a resumable copy — files transferred and
    skipped (already at the target) — driving its progress bar, monitor
    payloads and final summary.

    Also a :class:`Tally` for :func:`run_abortable`: :meth:`snapshot` counts
    skipped files as done.

    Parameters
    ----------
    name : str
        Client name, prefixed to the monitor messages.
    label : str
        Short name of what is copied, for the progress bar.
    progress : rich.progress.Progress
        Display receiving a listing task and a copy task.
    track_bytes : bool
        Bar in bytes instead of files.
    """

    def __init__(self, name: str, label: str, progress: Progress, *,
                 track_bytes: bool = False) -> None:
        self.name = name
        self.label = label
        self.files_total = 0
        self.bytes_total = 0
        self.transferred_files = 0
        self.transferred_bytes = 0
        self.skipped_files = 0
        self.skipped_bytes = 0
        self._start = time.monotonic()
        self._lock = threading.Lock()
        self._progress = progress
        self._track_bytes = track_bytes
        self.walk_task = progress.add_task(
            f"[magenta]Listing {label}[/magenta]", total=None, unit="files",
        )
        self._copy_task = progress.add_task(
            f"[cyan]Copying {label}[/cyan]", total=None,
            unit="bytes" if track_bytes else "files",
        )

    def add_total(self, n_files: int, n_bytes: int) -> None:
        """Count *n_files* / *n_bytes* more listed at the source."""
        with self._lock:
            self.files_total += n_files
            self.bytes_total += n_bytes
            self._progress.update(
                self._copy_task,
                total=self.bytes_total if self._track_bytes else self.files_total,
            )

    def listed(self) -> None:
        """The source listing is complete: drop its progress task."""
        if self.walk_task is not None:
            self._progress.remove_task(self.walk_task)
            self.walk_task = None

    def advance(self, is_skip: bool, n_bytes: int, n_files: int = 1) -> None:
        """Count files transferred, or skipped when *is_skip*."""
        with self._lock:
            if is_skip:
                self.skipped_files += n_files
                self.skipped_bytes += n_bytes
            else:
                self.transferred_files += n_files
                self.transferred_bytes += n_bytes
            if self._track_bytes:
                self._progress.advance(self._copy_task, n_bytes)
            else:
                self._progress.update(
                    self._copy_task,
                    completed=self.transferred_files + self.skipped_files,
                )
            if self.skipped_files:
                self._progress.update(
                    self._copy_task,
                    description=(
                        f"[cyan]Copying {self.label}[/cyan] "
                        f"[dim]({self.skipped_files} skipped)[/dim]"
                    ),
                )

    def snapshot(self) -> tuple[int, int, int]:
        """``(bytes_done, bytes_total, files_done)``, skipped files included."""
        with self._lock:
            return (self.transferred_bytes + self.skipped_bytes,
                    self.bytes_total,
                    self.transferred_files + self.skipped_files)

    def elapsed(self) -> float:
        return time.monotonic() - self._start

    def rate(self) -> float:
        """Transfer rate in bytes/s (skipped files excluded)."""
        elapsed = self.elapsed()
        return self.transferred_bytes / elapsed if elapsed > 0 else 0

    def payload(self) -> dict:
        """The structured ``progress`` field of a monitor event."""
        with self._lock:
            done = self.transferred_bytes + self.skipped_bytes
            return {
                "label": f"{self.name}: {self.transferred_files} files",
                "value": done,
                "maximum": self.bytes_total or None,
                "unit": "byte",
                "total_transferred": done,
                "files_done": self.transferred_files,
                "files_skipped": self.skipped_files,
                "rate": self.rate(),
            }

    def _skip_suffix(self) -> str:
        if not self.skipped_files:
            return ""
        return (f" [{self.skipped_files} skipped, "
                f"{_fmt_binary(self.skipped_bytes)}]")

    async def heartbeat(self, monitor) -> None:
        """Send the periodic progress event to *monitor*."""
        await monitor.emit(
            f"{self.name}: {self.transferred_files} files "
            f"({_fmt_binary(self.transferred_bytes)}, "
            f"{_fmt_binary(self.rate())}/s)" + self._skip_suffix(),
            status="progress",
            progress=self.payload(),
        )

    async def finish(self, monitor, console: Console) -> None:
        """Print the summary on *console* and send the final ``ok`` event."""
        if self.skipped_files:
            logging.getLogger(__name__).info(
                "Skipped %d already-complete file(s) (%s)",
                self.skipped_files, _fmt_binary(self.skipped_bytes),
            )
            console.print(
                f"Skipped [bold]{self.skipped_files}[/bold] already-complete "
                f"file(s) ([bold]{_fmt_binary(self.skipped_bytes)}[/bold])"
            )
        elapsed, rate = self.elapsed(), self.rate()
        console.print(
            f"Transferred [bold]{_fmt_binary(self.transferred_bytes)}[/bold] "
            f"in [bold]{elapsed:.1f}s[/bold] "
            f"([bold]{_fmt_binary(rate)}/s[/bold])"
        )
        await monitor.emit(
            f"{self.name}: Transferred {_fmt_binary(self.transferred_bytes)} "
            f"in {elapsed:.1f}s ({_fmt_binary(rate)}/s)" + self._skip_suffix(),
            status="ok",
            progress=self.payload(),
        )

    def result(self) -> dict:
        """The result dict of the copy functions of :mod:`nexus_transfers.api`."""
        return {
            "bytes": self.bytes_total,
            "files": self.files_total,
            "transferred_bytes": self.transferred_bytes,
            "transferred_files": self.transferred_files,
            "skipped_bytes": self.skipped_bytes,
            "skipped_files": self.skipped_files,
        }


class Tally:
    """Thread-safe ``(bytes_done, bytes_total, files_done)`` counters, in the
    shape of the :mod:`nexus_transfers.api` progress callback."""

    def __init__(self, bytes_total: int = 0) -> None:
        self.bytes_total = bytes_total
        self.bytes_done = 0
        self.files_done = 0
        self._lock = threading.Lock()

    def add(self, n_bytes: int, n_files: int = 1) -> None:
        with self._lock:
            self.bytes_done += n_bytes
            self.files_done += n_files

    def snapshot(self) -> tuple[int, int, int]:
        with self._lock:
            return self.bytes_done, self.bytes_total, self.files_done


async def run_abortable(coro, tally: Tally, progress, interval: float):
    """Await *coro*, calling ``progress(*tally.snapshot())`` every *interval*
    seconds and once at the end.

    If ``progress`` raises, the work is cancelled and that exception
    propagates — the abort contract of :mod:`nexus_transfers.api`.  Without a
    ``progress`` callback this is a plain ``await``.
    """
    import asyncio

    task = asyncio.ensure_future(coro)
    if progress is None:
        return await task
    failure: list = []

    async def _ticker() -> None:
        while not task.done():
            await asyncio.wait([task], timeout=interval)
            if task.done():
                return
            try:
                progress(*tally.snapshot())
            except BaseException as exc:  # noqa: BLE001 - the caller's abort
                failure.append(exc)
                task.cancel()
                return

    ticker = asyncio.create_task(_ticker())
    try:
        try:
            result = await task
        except asyncio.CancelledError:
            if not failure:
                raise
        await ticker
        if failure:
            raise failure[0]
    finally:
        if not task.done():
            task.cancel()
            try:
                await task
            except BaseException:  # noqa: BLE001 - already failing
                pass
        if not ticker.done():
            ticker.cancel()
    progress(*tally.snapshot())
    return result
