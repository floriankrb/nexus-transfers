"""Atomic file I/O helpers."""

import os
import tempfile

# Downloaded files always get 644, regardless of the process umask, so
# transferred datasets end up world-readable on every site.
_FILE_MODE = 0o644


def _trunc(s, limit=200):
    s = str(s)
    return s[:limit] + "..." if len(s) > limit else s


def _write_file(path, data):
    """Write *data* to *path* atomically via a temp file + rename."""
    dirpath = os.path.dirname(path)
    if not dirpath:
        raise ValueError(
            f"path {path!r} has no directory component; a dir-qualified "
            "(ideally absolute) path is required — refusing to write to cwd."
        )
    fd, tmp = tempfile.mkstemp(dir=dirpath)
    os.fchmod(fd, _FILE_MODE)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
        os.replace(tmp, path)
    except BaseException:
        os.unlink(tmp)
        raise
