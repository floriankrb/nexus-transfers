"""Atomic file I/O helpers."""

import os
import tempfile


def _trunc(s, limit=200):
    s = str(s)
    return s[:limit] + "..." if len(s) > limit else s


def _write_file(path, data):
    """Write *data* to *path* atomically via a temp file + rename."""
    dirpath = os.path.dirname(path)
    fd, tmp = tempfile.mkstemp(dir=dirpath)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
        os.replace(tmp, path)
    except BaseException:
        os.unlink(tmp)
        raise


def _move_file_atomic(src, dst):
    """Move *src* to *dst* atomically via ``os.replace``.

    Requires *src* and *dst* to be on the same filesystem.
    """
    os.replace(src, dst)
