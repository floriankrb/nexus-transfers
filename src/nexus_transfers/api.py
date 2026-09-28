"""Importable transfer API: what other tools call instead of the CLI.

The functions here are the stable entry points for a program that drives a
transfer itself (e.g. ``anemoi-nexus-client``, which records the transfer in
the Nexus catalogue).  They are plain synchronous calls — each runs its own
event loop — with an ``a``-prefixed async form each:

==============================  ===========================================
:func:`push_ssh`                local → ``[user@]host:/path`` (resumable)
:func:`pull_ssh`                ``[user@]host:/path`` → local (resumable)
:func:`verify_ssh`              compare a remote copy with what is expected
:func:`delete_ssh`              remove a remote copy (with safety checks)
:func:`stat_ssh`                totals of what is at a remote path
==============================  ===========================================

The contract, common to every call that takes ``progress``:

* **Progress** — ``progress(bytes_done, bytes_total, files_done)`` is called
  every ``progress_interval`` seconds and once at the end.
* **Abort** — **raising from** ``progress`` **stops the work** (the workers
  are cancelled, connections closed) **and the exception propagates** to the
  caller unchanged.  This is how a caller stops work it no longer owns (e.g.
  its Nexus lease was taken over).  Work already done stays done (files
  already copied or deleted), which the resumable / idempotent calls make
  harmless on a re-run.
* **Lock** (``push_ssh`` / ``pull_ssh``) — ``lock`` names the transfer on
  the broker: the relay enforces unique client names, so registering under
  ``lock`` makes it a distributed mutex.  A stale holder of the same name (a
  previous, hung run of the same transfer) is displaced — soft kill, then
  hard kill — before this one starts, and losing a race for the name raises
  :class:`~nexus_transfers.client.NameTakenError`.  Key it on the
  transfer's identity (the Nexus client uses
  ``nexus-location-<location_uuid>``) and at most one run of that transfer
  is ever active.  Without a broker there is no lock (the caller's own
  protocol must then exclude concurrent runs).

::

    from nexus_transfers.api import push_ssh, stat_ssh, verify_ssh

    result = push_ssh("/data/x.zarr", "user@host:/remote/x.zarr",
                      progress=print, lock="nexus-location-<location_uuid>")
    # {"bytes": …, "files": …, "transferred_bytes": …, "skipped_files": …}
    stat_ssh("user@host:/remote/x.zarr")   # {"bytes": …, "files": …} or None
    verify_ssh("user@host:/remote/x.zarr", "/data/x.zarr")
    # {"bytes", "files", "missing": [...], "mismatched": [...], "extra": [...]}
"""

from __future__ import annotations

import asyncio
import logging
import stat as _stat
from typing import Callable

from nexus_transfers.config import broker_url_default

_logger = logging.getLogger(__name__)

#: ``progress(bytes_done, bytes_total, files_done)``; may raise to abort.
Progress = Callable[[int, int, int], None]


def default_broker_url() -> str | None:
    """The broker URL from the environment / config file, or ``None``.

    Same resolution as the ``copy-ssh`` command's ``--broker-url`` default:
    ``$NEXUS_TRANSFERS_URL``, else ``[nexus.copy_ssh] broker_url``, else
    ``[nexus] url`` of the config file."""
    return broker_url_default("copy_ssh")


async def apush_ssh(
    source: str,
    target: str,
    *,
    progress: Progress | None = None,
    progress_interval: float = 5.0,
    lock: str | None = None,
    broker_url: str | None = None,
    ssl_verify: bool = True,
    site: str | None = None,
    max_concurrent: int = 4,
    ssh_port: int = 22,
    ssh_key: str | None = None,
    ssh_connections: int = 2,
    processes: int = 1,
    stat_concurrency: int = 64,
    encryption_algs: list[str] | None = None,
    quiet: bool = True,
) -> dict:
    """Async form of :func:`push_ssh`."""
    import uuid

    from nexus_transfers.copy_ssh import _copy_to_ssh

    if lock and not broker_url:
        _logger.warning("no broker configured: transfer %r runs without the broker lock", lock)
    name = lock or f"push-{uuid.uuid4().hex[:8]}"
    return await _copy_to_ssh(
        source=source, target=target, broker_url=broker_url, name=name, site=site,
        max_concurrent=max_concurrent, ssh_port=ssh_port, ssh_key=ssh_key,
        ssh_connections=ssh_connections, track_bytes=True, ssl_verify=ssl_verify,
        quiet=quiet, processes=processes, stat_concurrency=stat_concurrency,
        encryption_algs=encryption_algs, steal=bool(lock and broker_url),
        progress_callback=progress, progress_interval=progress_interval,
    )


def push_ssh(source: str, target: str, **kwargs) -> dict:
    """Copy the local file or directory *source* to the SSH *target*
    (``[user@]host:/path``), resumably (files already there with the same
    size are skipped).

    Keyword arguments: ``progress`` / ``progress_interval`` (the module
    docstring's contract), ``lock`` (the broker lock name),
    ``broker_url`` (for the lock and live monitoring; ``None`` = none),
    ``ssl_verify``, ``site`` (label for monitor messages), and the transfer
    tuning of ``nexus-transfers copy-ssh``: ``max_concurrent``, ``ssh_port``,
    ``ssh_key``, ``ssh_connections``, ``processes``, ``stat_concurrency``,
    ``encryption_algs``, ``quiet``.

    Returns ``{"bytes", "files", "transferred_bytes", "transferred_files",
    "skipped_bytes", "skipped_files"}``.  Raises
    :class:`~nexus_transfers.client.NameTakenError` if the lock cannot be
    taken, and whatever ``progress`` raised if it aborted the copy.
    """
    return asyncio.run(apush_ssh(source, target, **kwargs))


async def apull_ssh(
    source: str,
    target: str,
    *,
    progress: Progress | None = None,
    progress_interval: float = 5.0,
    lock: str | None = None,
    broker_url: str | None = None,
    ssl_verify: bool = True,
    site: str | None = None,
    max_concurrent: int = 4,
    ssh_port: int = 22,
    ssh_key: str | None = None,
    ssh_connections: int = 2,
    processes: int = 1,
    stat_concurrency: int = 64,
    encryption_algs: list[str] | None = None,
    quiet: bool = True,
) -> dict:
    """Async form of :func:`pull_ssh`."""
    import uuid

    from nexus_transfers.ssh_ops import _copy_from_ssh

    if lock and not broker_url:
        _logger.warning("no broker configured: transfer %r runs without the broker lock", lock)
    # processes / stat_concurrency / quiet: accepted for symmetry with
    # push_ssh; a pull runs in one process and prints nothing.
    name = lock or f"pull-{uuid.uuid4().hex[:8]}"
    return await _copy_from_ssh(
        source, target, broker_url=broker_url, name=name, site=site,
        max_concurrent=max_concurrent, ssh_port=ssh_port, ssh_key=ssh_key,
        ssh_connections=ssh_connections, ssl_verify=ssl_verify,
        encryption_algs=encryption_algs, steal=bool(lock and broker_url),
        progress_callback=progress, progress_interval=progress_interval,
    )


def pull_ssh(source: str, target: str, **kwargs) -> dict:
    """Copy the SSH *source* (``[user@]host:/path``, a file or a directory)
    to the local *target*, resumably (local files that already have the
    remote size are skipped; each file is written to ``<name>.<8hex>.tmp``
    and renamed).  A single remote file is copied *to* *target*.

    Keyword arguments and result: as for :func:`push_ssh` (``bytes`` /
    ``files`` are the source's totals).  Raises ``FileNotFoundError`` if
    nothing is at *source*, :class:`~nexus_transfers.client.NameTakenError`
    if the lock cannot be taken, and whatever ``progress`` raised.
    """
    return asyncio.run(apull_ssh(source, target, **kwargs))


async def averify_ssh(
    target: str,
    expected,
    *,
    checksum: bool = False,
    progress: Progress | None = None,
    progress_interval: float = 5.0,
    ssh_port: int = 22,
    ssh_key: str | None = None,
    ssh_connections: int = 2,
    max_concurrent: int = 4,
    encryption_algs: list[str] | None = None,
) -> dict:
    """Async form of :func:`verify_ssh`."""
    from nexus_transfers._progress import Tally, run_abortable
    from nexus_transfers.check_files_ssh import _verify_ssh

    tally = Tally()
    return await run_abortable(
        _verify_ssh(target, expected, checksum=checksum, tally=tally,
                    ssh_port=ssh_port, ssh_key=ssh_key,
                    ssh_connections=ssh_connections,
                    max_concurrent=max_concurrent,
                    encryption_algs=encryption_algs),
        tally, progress, progress_interval,
    )


def verify_ssh(target: str, expected, **kwargs) -> dict:
    """Compare what is at the SSH *target* with what is *expected* —
    read-only (nothing is fixed or deleted).

    *expected* is a local reference path (a directory, or a single file) or a
    manifest ``{relative_posix_path: size}``; a single-file *target* is
    compared as the manifest's one file.  Files are compared by size; with
    ``checksum=True`` and a local reference, same-size files also by MD5
    (``md5sum`` must exist on the remote host).

    Returns ``{"bytes", "files", "missing", "mismatched", "extra"}``: the
    totals of what is at *target* (as :func:`stat_ssh`), and sorted lists of
    relative paths.  Nothing at *target* → ``bytes = files = 0`` and every
    expected file missing.  The copy is complete when ``missing`` and
    ``mismatched`` are empty.

    Keyword arguments: ``checksum``, ``progress`` / ``progress_interval``
    (``bytes_done`` counts expected bytes compared), ``ssh_port``,
    ``ssh_key``, ``ssh_connections``, ``max_concurrent`` (parallel hashes),
    ``encryption_algs``.
    """
    return asyncio.run(averify_ssh(target, expected, **kwargs))


async def adelete_ssh(
    target: str,
    *,
    root: str | None = None,
    progress: Progress | None = None,
    progress_interval: float = 5.0,
    ssh_port: int = 22,
    ssh_key: str | None = None,
) -> dict:
    """Async form of :func:`delete_ssh`."""
    from nexus_transfers.ssh_ops import _delete_ssh

    return await _delete_ssh(target, root=root, progress=progress,
                             progress_interval=progress_interval,
                             ssh_port=ssh_port, ssh_key=ssh_key)


def delete_ssh(target: str, **kwargs) -> dict:
    """Remove the file or directory tree at the SSH *target*; idempotent.

    Refuses, with ``ValueError`` and before connecting: an empty, ``.`` /
    ``..`` or relative path, a path with fewer than three components,
    ``/`` included (``/``, ``/a``), and — when ``root`` (the storage root) is
    given — the root itself or a path outside it.  Symlinks are removed,
    never followed.

    Returns ``{"bytes", "files"}`` removed (``0`` / ``0`` if nothing was
    there).  Keyword arguments: ``root``, ``progress`` /
    ``progress_interval``, ``ssh_port``, ``ssh_key``.
    """
    return asyncio.run(adelete_ssh(target, **kwargs))


async def astat_ssh(target: str, *, ssh_port: int = 22, ssh_key: str | None = None) -> dict | None:
    """Async form of :func:`stat_ssh`."""
    from nexus_transfers.ssh import parse_ssh_target
    from nexus_transfers.ssh import SSHPool, walk_remote

    user, host, path = parse_ssh_target(target)
    async with SSHPool(host, ssh_port, user, ssh_key, 1) as pool:
        sftp = pool.get_sftp()
        try:
            attrs = await sftp.stat(path)
        except Exception:                 # asyncssh.SFTPError: nothing there
            return None
        if not _stat.S_ISDIR(attrs.permissions or 0):
            return {"bytes": int(attrs.size or 0), "files": 1}
        files = await walk_remote(sftp, path)
    return {"bytes": sum(size for _, size in files), "files": len(files)}


def stat_ssh(target: str, **kwargs) -> dict | None:
    """What is at the SSH *target*: ``{"bytes", "files"}`` (a file counts as
    one), or ``None`` if nothing is there.  ``ssh_port`` / ``ssh_key`` as for
    :func:`push_ssh`."""
    return asyncio.run(astat_ssh(target, **kwargs))

