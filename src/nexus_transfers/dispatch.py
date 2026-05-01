"""Dispatch table – functions exposed by every client."""

import logging
import math
import os

logger = logging.getLogger(__name__)


def adder(value):
    """Add 1 to a numeric value.

    Parameters
    ----------
    value
        A number to increment.
    """
    return value + 1


def echo(*args):
    """Return the arguments unchanged.

    Parameters
    ----------
    *args
        Arbitrary positional arguments.
    """
    if len(args) == 1:
        return args[0]
    return list(args)


class FileTransfer:
    """Marker returned by ``get_file`` to trigger binary chunk transfer.

    Parameters
    ----------
    path
        Path to the file to transfer.
    chunk_size
        Size of each chunk in bytes.
    """

    def __init__(self, path, chunk_size=65536):
        self.path = os.path.abspath(path)
        self.size = os.path.getsize(self.path)
        self.chunk_size = chunk_size
        self.total_chunks = math.ceil(self.size / chunk_size) if self.size > 0 else 0


def resolve_safe_path(path, allowed_paths):
    """Resolve a path and verify it is within an allowed directory.

    Parameters
    ----------
    path
        The path to resolve (may be relative or absolute).
    allowed_paths
        List of allowed base directories.

    Raises
    ------
    PermissionError
        If the resolved path is not under any allowed directory.
    ValueError
        If the path contains ``..`` components.
    """
    if ".." in os.path.normpath(path).split(os.sep):
        raise ValueError(f"path contains '..': {path}")

    resolved = os.path.realpath(path)

    for allowed in allowed_paths:
        allowed_resolved = os.path.realpath(allowed)
        # Ensure trailing separator for prefix check
        if resolved == allowed_resolved or resolved.startswith(allowed_resolved + os.sep):
            return resolved

    raise PermissionError(f"path '{path}' (resolved to '{resolved}') is outside allowed directories")


def make_get_file(allowed_paths):
    """Create a ``get_file`` function bound to allowed paths.

    Parameters
    ----------
    allowed_paths
        List of allowed base directories.
    """
    def get_file(path, chunk_size=65536):
        """Read a file and return it for binary transfer.

        Parameters
        ----------
        path
            Path to the file to read.
        chunk_size
            Size of each binary chunk in bytes (chosen by the caller).
        """
        resolved = resolve_safe_path(path, allowed_paths)
        return FileTransfer(resolved, chunk_size=chunk_size)

    return get_file


def make_list_dir(allowed_paths):
    """Create a ``list_dir`` function bound to allowed paths.

    Parameters
    ----------
    allowed_paths
        List of allowed base directories.
    """
    def list_dir(path=".", include_size=False, offset=0, limit=1000):
        """List the contents of a directory with pagination.

        Parameters
        ----------
        path
            Path to the directory to list.
        include_size
            Whether to include file sizes in the result.
        offset
            Index of the first entry to return.
        limit
            Maximum number of entries to return.
        """
        logger.info("list_dir called with path=%r, include_size=%s, offset=%d, limit=%d", path, include_size, offset, limit)
        resolved = resolve_safe_path(path, allowed_paths)
        logger.info("Resolved path: %s", resolved)
        if not os.path.isdir(resolved):
            raise NotADirectoryError(f"not a directory: {path}")
        entries = []
        with os.scandir(resolved) as it:
            for entry in sorted(it, key=lambda e: e.name):
                info = {
                    "name": entry.name,
                    "type": "dir" if entry.is_dir(follow_symlinks=False) else "file",
                }
                if include_size and entry.is_file(follow_symlinks=False):
                    info["size"] = entry.stat(follow_symlinks=False).st_size
                entries.append(info)
        page = entries[offset:offset + limit]
        logger.info("Returning %d/%d entries (offset=%d) in %s", len(page), len(entries), offset, resolved)
        return page

    return list_dir


DISPATCH = {
    "adder": adder,
    "echo": echo,
}
