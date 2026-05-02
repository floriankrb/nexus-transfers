"""Terminal progress bar for file transfers."""
from __future__ import annotations

import math
import sys
import threading
import time
from typing import IO


class Progress:
    """
    Thread-safe progress reporter that renders a single-line progress bar
    to an output stream (default: stderr) at ~5 Hz.

    Call order:
        p = Progress()
        p.set_manifest(n_files, total_bytes)
        for each file:
            p.start_file(path, size, resume_offset)
            p.add_bytes(chunk_size)  # called many times
            p.finish_file()
        p.stop()
    """

    def __init__(self, out: IO[str] | None = None) -> None:
        self._out = out or sys.stderr
        self._lock = threading.Lock()

        self._total_files = 0
        self._total_bytes = 0
        self._done_files = 0
        self._done_bytes = 0
        self._current_file = ""

        self._start = time.monotonic()
        self._stop_ev = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    # ── public API ────────────────────────────────────────────────────────────

    def set_manifest(self, total_files: int, total_bytes: int) -> None:
        with self._lock:
            self._total_files = total_files
            self._total_bytes = total_bytes

    def start_file(self, path: str, size: int, offset: int = 0) -> None:
        with self._lock:
            self._current_file = path

    def add_bytes(self, n: int) -> None:
        with self._lock:
            self._done_bytes += n

    def finish_file(self) -> None:
        with self._lock:
            self._done_files += 1

    def stop(self) -> None:
        self._stop_ev.set()
        self._thread.join()
        self._render()                   # final render
        print(file=self._out)            # newline after bar

    # ── internal ──────────────────────────────────────────────────────────────

    def _loop(self) -> None:
        while not self._stop_ev.wait(timeout=0.2):
            self._render()

    def _render(self) -> None:
        with self._lock:
            done_b = self._done_bytes
            total_b = self._total_bytes
            done_f = self._done_files
            total_f = self._total_files
            cur = self._current_file

        elapsed = time.monotonic() - self._start
        speed_mbs = (done_b / elapsed / 1e6) if elapsed > 0 else 0.0

        pct = min(done_b / total_b, 1.0) if total_b > 0 else 0.0

        if speed_mbs > 0 and total_b > done_b:
            eta_s = (total_b - done_b) / 1e6 / speed_mbs
            eta = "ETA " + _fmt_duration(eta_s)
        elif done_b >= total_b > 0:
            eta = "done"
        else:
            eta = "ETA ?"

        bar = _render_bar(pct, 20)
        cur_short = ("…" + cur[-(34):]) if len(cur) > 35 else cur

        line = (
            f"\r{bar} {pct*100:3.0f}% | "
            f"{done_f}/{total_f} files | "
            f"{_fmt_bytes(done_b)}/{_fmt_bytes(total_b)} | "
            f"{speed_mbs:.1f} MB/s | "
            f"{eta} | {cur_short}"
        )
        print(line, end="", file=self._out, flush=True)


def _render_bar(pct: float, width: int) -> str:
    filled = round(pct * width)
    return "[" + "█" * filled + "░" * (width - filled) + "]"


def _fmt_bytes(n: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} {unit}"
        n /= 1024
    return f"{n:.1f} PiB"


def _fmt_duration(s: float) -> str:
    s = int(s)
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    if h:
        return f"{h}h{m:02d}m{s:02d}s"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"
