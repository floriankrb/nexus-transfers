"""Pull from and delete on an SSH target: the counterparts of ``copy-ssh``.

Used by :mod:`nexus_transfers.api` (``pull_ssh`` / ``delete_ssh``); there
is no CLI command for them.
"""

import logging
import os
import posixpath
import stat as _stat
from pathlib import PurePosixPath
from typing import Callable

from nexus_transfers._progress import CopyStats, Tally, make_progress, run_abortable
from nexus_transfers._run import Monitor, run_workers
from nexus_transfers.ssh import parse_ssh_target
from nexus_transfers.ssh import SSHPool, read_file, walk_remote, walk_remote_tree

_logger = logging.getLogger(__name__)


def check_deletable(path: str, root: str | None = None) -> str:
    """Refuse a remote *path* that a delete must never touch; return it
    normalised.

    Refused (``ValueError``): an empty path, ``.`` / ``..``, a relative path
    (it would be resolved against the remote home), a path with fewer than
    three components counting ``/`` (``/``, ``/a``: the rule of
    the Nexus client's file mover), and — when *root* (the
    storage root) is given — the root itself or anything outside it.  A
    malformed location must not turn into an ``rm -rf`` of a whole storage.
    """
    raw = (path or "").strip()
    if raw in ("", ".", ".."):
        raise ValueError(f"refusing to delete {path!r}: no path")
    if not raw.startswith("/"):
        raise ValueError(f"refusing to delete {path!r}: not an absolute path")
    norm = posixpath.normpath(raw)
    if len(PurePosixPath(norm).parts) < 3:
        raise ValueError(f"refusing to delete {norm}: too close to the filesystem root")
    if root:
        nroot = posixpath.normpath(root.strip())
        if norm == nroot:
            raise ValueError(f"refusing to delete {norm}: it is the storage root")
        if not norm.startswith(nroot.rstrip("/") + "/"):
            raise ValueError(f"refusing to delete {norm}: not under the storage root {nroot}")
    return norm


async def _delete_ssh(
    target: str,
    *,
    root: str | None = None,
    progress: Callable[[int, int, int], None] | None = None,
    progress_interval: float = 5.0,
    ssh_port: int = 22,
    ssh_key: str | None = None,
) -> dict:
    """Remove the file or directory tree at the SSH *target*.

    Safety checks (:func:`check_deletable`) run before connecting.  Files
    (and symlinks, never followed) are removed first, then directories
    deepest first.  Nothing there is not an error.

    Returns ``{"bytes", "files"}`` removed.
    """
    user, host, path = parse_ssh_target(target)
    path = check_deletable(path, root)
    tally = Tally()

    async def _body() -> None:
        async with SSHPool(host, ssh_port, user, ssh_key, 1) as pool:
            sftp = pool.get_sftp()
            try:
                attrs = await sftp.lstat(path)
            except Exception:             # asyncssh.SFTPError: nothing there
                return
            if not _stat.S_ISDIR(attrs.permissions or 0):
                tally.bytes_total = int(attrs.size or 0)
                await sftp.remove(path)
                tally.add(int(attrs.size or 0))
                return
            entries, dirs = await walk_remote_tree(sftp, path)
            tally.bytes_total = sum(size for _, size in entries)
            for entry, size in entries:
                await sftp.remove(entry)
                tally.add(size)
            for d in dirs:
                await sftp.rmdir(d)
            await sftp.rmdir(path)

    await run_abortable(_body(), tally, progress, progress_interval)
    done_bytes, _, done_files = tally.snapshot()
    return {"bytes": done_bytes, "files": done_files}


async def _copy_from_ssh(
    source: str,
    target: str,
    *,
    broker_url: str | None,
    name: str,
    site: str | None = None,
    max_concurrent: int = 4,
    ssh_port: int = 22,
    ssh_key: str | None = None,
    ssh_connections: int = 2,
    ssl_verify: bool = True,
    encryption_algs: list[str] | None = None,
    steal: bool = False,
    progress_callback: Callable[[int, int, int], None] | None = None,
    progress_interval: float = 5.0,
) -> dict:
    """Copy the SSH *source* (``[user@]host:/path``, a directory or a single
    file) to the local *target*, resumably: a local file that already has the
    remote size is skipped.  Each file is downloaded to ``<name>.<8hex>.tmp``
    and renamed into place.

    *broker_url* / *name* / *steal*: as for
    :func:`nexus_transfers.copy_ssh._copy_to_ssh` (the name is the lock).

    Returns ``{"bytes", "files", "transferred_bytes", "transferred_files",
    "skipped_bytes", "skipped_files"}`` — ``bytes`` / ``files`` are the
    totals of the source.  Raises ``FileNotFoundError`` if nothing is at
    *source*.
    """
    user, host, remote_base = parse_ssh_target(source)
    remote_base = remote_base.rstrip("/") or "/"
    target = os.path.expanduser(target)
    dest_label = f"{site}:{remote_base}" if site else f"{host}:{remote_base}"

    # No console output here: the display is only the counters' carrier.
    stats = CopyStats(name, dest_label, make_progress(quiet=True))

    async def _body() -> None:
        async with SSHPool(host, ssh_port, user, ssh_key, ssh_connections,
                           encryption_algs) as pool:
            sftp = pool.get_sftp()
            try:
                attrs = await sftp.stat(remote_base)
            except Exception as exc:      # asyncssh.SFTPError
                raise FileNotFoundError(f"nothing at {source}") from exc
            if _stat.S_ISDIR(attrs.permissions or 0):
                files = await walk_remote(sftp, remote_base)
                items = [(f"{remote_base}/{rel}", os.path.join(target, *rel.split("/")), size)
                         for rel, size in files]
            else:
                items = [(remote_base, target, int(attrs.size or 0))]
            stats.add_total(len(items), sum(size for *_, size in items))

            async def _pull(item: tuple[str, str, int]) -> None:
                remote_file, local_file, size = item
                if os.path.isfile(local_file) and os.path.getsize(local_file) == size:
                    stats.advance(True, size)
                else:
                    await read_file(pool.get_sftp(), remote_file, local_file)
                    stats.advance(False, size)

            await run_workers(items, _pull, max_concurrent)

    async with await Monitor.connect(name, broker_url, ssl_verify=ssl_verify,
                                     steal=steal) as monitor:
        await monitor.emit(f"{name}: starting pull {dest_label} -> {target}",
                           status="progress")
        await run_abortable(_body(), stats, progress_callback, progress_interval)
        await monitor.emit(
            f"{name}: pulled {stats.transferred_files} files "
            f"({stats.skipped_files} skipped)", status="ok",
            progress={"label": f"{name}: {stats.files_total} files",
                      "value": stats.bytes_total,
                      "maximum": stats.bytes_total or None,
                      "unit": "byte"},
        )
    return stats.result()
