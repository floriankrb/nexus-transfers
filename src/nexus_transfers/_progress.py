"""Shared progress-bar helpers used by client.py and copy_ssh.py."""

from rich.progress import ProgressColumn
from rich.text import Text


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
