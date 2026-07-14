"""Atomic file I/O helpers."""

import os
import uuid

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
    # "<name>.<8 hex>.tmp" matches check_files' leftover pattern (same
    # naming as ssh.write_file), so debris from a crash between write and
    # rename is deletable by --delete-extra.
    tmp = os.path.join(dirpath, f"{os.path.basename(path)}.{uuid.uuid4().hex[:8]}.tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, _FILE_MODE)
    os.fchmod(fd, _FILE_MODE)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
        os.replace(tmp, path)
    except BaseException:
        os.unlink(tmp)
        raise
