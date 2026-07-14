"""Dispatch table – functions exposed by every client."""

import hashlib
import logging
import math
import os
import stat as _stat
import threading
from collections import OrderedDict

logger = logging.getLogger(__name__)

# Maximum number of directory snapshots kept by ``make_list_dir``'s
# pagination cache (LRU). Each snapshot is a sorted list of entry names;
# even a huge zarr chunk directory costs only tens of MB.
_LIST_DIR_CACHE_SIZE = 16


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


class S3Transfer:
    """Marker returned by ``get_file(use_s3=True)`` to trigger S3 staging.

    The ``Client`` running on the source uploads ``local_path`` to the
    configured S3 bucket and replies with the object key, size, and
    checksum.  No payload is sent over the WebSocket itself.

    Parameters
    ----------
    local_path
        Absolute path of the file to upload.
    s3_prefix
        Optional prefix prepended to the S3 key (e.g. a per-transfer
        directory name).
    """

    def __init__(self, local_path: str, s3_prefix: str | None = None):
        self.local_path = os.path.abspath(local_path)
        self.s3_prefix = s3_prefix


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
    def get_file(path, chunk_size=65536, use_s3=True, s3_prefix=None):
        """Read a file and return it for transfer.

        Parameters
        ----------
        path
            Path to the file to read.
        chunk_size
            Size of each binary chunk in bytes (only used when ``use_s3``
            is False).
        use_s3
            If True (default), stage the file via the configured S3 bucket.
            Set to False to send binary chunks over the WebSocket.
        s3_prefix
            Optional prefix prepended to the S3 object key.
        """
        resolved = resolve_safe_path(path, allowed_paths)
        if use_s3:
            return S3Transfer(resolved, s3_prefix=s3_prefix)
        return FileTransfer(resolved, chunk_size=chunk_size)

    return get_file


def compute_file_hash(path, algo="md5", chunk_size=1024 * 1024):
    """Compute the hex digest of a file, reading it in chunks.

    Parameters
    ----------
    path
        Path to the file to hash.
    algo
        Any algorithm name accepted by :func:`hashlib.new` (default md5,
        used for corruption detection, not security).
    chunk_size
        Read size in bytes.
    """
    hasher = hashlib.new(algo)
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def make_hash_file(allowed_paths):
    """Create a ``hash_file`` function bound to allowed paths.

    Parameters
    ----------
    allowed_paths
        List of allowed base directories.
    """
    def hash_file(path, algo="md5"):
        """Return the hash, size and permission bits of a file.

        Parameters
        ----------
        path
            Path to the file to hash.
        algo
            Hash algorithm name accepted by :func:`hashlib.new`
            (default md5 — corruption detection, not security).
        """
        resolved = resolve_safe_path(path, allowed_paths)
        if not os.path.isfile(resolved):
            raise FileNotFoundError(f"not a file: {path}")
        st = os.stat(resolved)
        return {
            "hash": compute_file_hash(resolved, algo=algo),
            "algo": algo,
            "size": st.st_size,
            "mode": _stat.S_IMODE(st.st_mode),
        }

    return hash_file


def make_list_dir(allowed_paths):
    """Create a ``list_dir`` function bound to allowed paths.

    The sorted name listing is cached per directory so that paginating
    through a huge directory does not rescan and re-sort all of it for
    every page (quadratic in the directory size). Every walk starts at
    ``offset == 0``, which always takes a fresh snapshot — so consecutive
    walks see current data, while the pages of one walk come from a single
    consistent snapshot (entries can no longer be skipped or duplicated
    when the directory changes mid-walk).

    Parameters
    ----------
    allowed_paths
        List of allowed base directories.
    """
    # resolved path -> sorted list of entry names, LRU-evicted. list_dir
    # runs in executor threads, so guard the cache with a lock.
    cache: OrderedDict = OrderedDict()
    lock = threading.Lock()

    def list_dir(path=".", include_size=False, offset=0, limit=1000,
                 progress_callback=None):
        """List the contents of a directory with pagination.

        Parameters
        ----------
        path
            Path to the directory to list.
        include_size
            Whether to include file sizes in the result.
        offset
            Index of the first entry to return. ``0`` takes a fresh
            directory snapshot; later pages are served from the snapshot.
        limit
            Maximum number of entries to return.
        progress_callback
            Optional callable invoked with the current entry count as
            files are scanned.
        """
        logger.debug("list_dir called with path=%r, include_size=%s, offset=%d, limit=%d", path, include_size, offset, limit)
        resolved = resolve_safe_path(path, allowed_paths)
        logger.debug("Resolved path: %s", resolved)
        if not os.path.isdir(resolved):
            raise NotADirectoryError(f"not a directory: {path}")

        names = None
        if offset > 0:
            with lock:
                names = cache.get(resolved)
                if names is not None:
                    cache.move_to_end(resolved)
        if names is None:
            # Names only: the scan stays a pure directory read (no per-entry
            # stat), so snapshotting costs the same as one page did before.
            names = []
            with os.scandir(resolved) as it:
                for entry in it:
                    names.append(entry.name)
                    # Throttled: a per-entry callback from this executor
                    # thread would hammer the GIL and starve the event loop.
                    if progress_callback is not None and len(names) % 1000 == 0:
                        progress_callback(len(names))
            names.sort()
            if progress_callback is not None:
                progress_callback(len(names))
            with lock:
                cache[resolved] = names
                cache.move_to_end(resolved)
                while len(cache) > _LIST_DIR_CACHE_SIZE:
                    cache.popitem(last=False)

        page = []
        for name in names[offset:offset + limit]:
            # One lstat per page entry replaces the DirEntry type/size
            # lookups (a snapshot cannot keep DirEntry objects). Symlink
            # semantics are unchanged: a symlink is always type "file".
            try:
                st = os.lstat(os.path.join(resolved, name))
            except OSError:
                # Vanished since the snapshot. Do not drop it: a short page
                # would end the caller's pagination early. Report it as a
                # file without size; downstream treats it like any missing
                # or changed file.
                page.append({"name": name, "type": "file"})
                continue
            info = {
                "name": name,
                "type": "dir" if _stat.S_ISDIR(st.st_mode) else "file",
            }
            if include_size and _stat.S_ISREG(st.st_mode):
                info["size"] = st.st_size
            page.append(info)

        logger.debug("Returning %d/%d entries (offset=%d) in %s",
                     len(page), len(names), offset, resolved)
        return page

    return list_dir


DISPATCH = {
    "adder": adder,
    "echo": echo,
}
