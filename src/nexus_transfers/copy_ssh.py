"""CLI tool: copy a local directory to a remote SSH/SFTP target.

Usage::

    nexus-transfers copy-ssh --source /data/dataset.zarr --target user@host:/remote/path
"""

import asyncio
import logging
import multiprocessing as mp
import os
import queue as _queue
import sys
import threading
import uuid
from pathlib import PurePosixPath
from typing import Callable

from nexus_transfers._progress import (
    CopyStats,
    make_console,
    make_progress,
    run_abortable,
    setup_cli_logging,
)
from nexus_transfers._run import Monitor, run_workers, ticking
from nexus_transfers._cli import CommandParser
from nexus_transfers.ssh import (
    SSHConfig,
    SSHPool,
    parse_ssh_target,
    stat_remote,
    write_file,
)

_logger = logging.getLogger(__name__)


async def _list_local(source_path: str) -> tuple[list[tuple[str, str, int]], int, int]:
    """Walk *source_path* and return ``(items, total_count, total_size)``.

    Each item is a ``(local_path, relative_path, size)`` tuple; a single-file
    *source_path* yields one item whose relative path is ``""``. Runs
    ``os.scandir`` in a thread-pool executor so NFS stat calls do not block the
    event loop.

    Parameters
    ----------
    source_path : str
        Root directory to walk.
    """
    loop = asyncio.get_running_loop()

    if os.path.isfile(source_path):
        # A single file: one item with an empty relative path (the target is
        # the file itself, not a directory to put it in).
        size = os.path.getsize(source_path)
        return [(source_path, "", size)], 1, size

    def _scan(dirpath: str, prefix: str) -> list[tuple[str, str, int]]:
        entries = []
        for entry in os.scandir(dirpath):
            rel = os.path.join(prefix, entry.name) if prefix else entry.name
            if entry.is_dir(follow_symlinks=False):
                entries.extend(_scan(entry.path, rel))
            else:
                try:
                    size = entry.stat().st_size
                except OSError:
                    size = 0
                entries.append((entry.path, rel, size))
        return entries

    items = await loop.run_in_executor(None, _scan, source_path, "")
    total_size = sum(size for _, _, size in items)
    return items, len(items), total_size


def _partition_by_bytes(
    items: list[tuple[str, str, int]], n: int,
) -> list[list[tuple[str, str, int]]]:
    """Split *items* into *n* shards balanced by total byte size.

    Greedy longest-processing-time bin-packing: largest files first, each
    assigned to the currently-lightest shard. Balancing by bytes rather than
    file count matters because chunk sizes vary widely (the statistics arrays
    are tiny next to ``data/`` chunks).

    Parameters
    ----------
    items : list of tuple
        ``(local_path, relative_path, size)`` tuples to distribute.
    n : int
        Number of shards.
    """
    shards: list[list[tuple[str, str, int]]] = [[] for _ in range(n)]
    loads = [0] * n
    for item in sorted(items, key=lambda it: it[2], reverse=True):
        idx = min(range(n), key=loads.__getitem__)
        shards[idx].append(item)
        loads[idx] += item[2]
    return shards


async def _run_shard(
    pool: SSHPool,
    items: list[tuple[str, str, int]],
    remote_base: str,
    report: Callable[[str, int, int], None],
    max_concurrent: int,
    stat_concurrency: int,
) -> None:
    """Stat-classify *items* then upload the ones that need it.

    The resume scan (remote ``stat`` per file) runs at ``stat_concurrency``
    depth — much deeper than the upload concurrency — and feeds an upload queue
    consumed by ``max_concurrent`` workers, so latency-bound stats overlap with
    bandwidth-bound uploads instead of serialising 8-deep.

    Parameters
    ----------
    pool : SSHPool
        Connection pool to draw SFTP clients from.
    items : list of tuple
        ``(local_path, relative_path, size)`` tuples for this shard.
    remote_base : str
        Remote destination root; ``rel_path`` is appended to it.
    report : callable
        ``report(kind, n_bytes, n_files)`` with ``kind`` in
        ``{"uploaded", "skipped"}``; called as work completes.
    max_concurrent : int
        Number of parallel upload workers.
    stat_concurrency : int
        Maximum number of concurrent remote ``stat`` calls.
    """
    sem = asyncio.Semaphore(stat_concurrency)

    async def _classify(item: tuple[str, str, int], put) -> None:
        local_file, rel_path, size = item
        rel_posix = PurePosixPath(rel_path).as_posix() if rel_path else ""
        # An empty relative path is a single-file source: the target *is* the file.
        remote_path = f"{remote_base}/{rel_posix}" if rel_posix else remote_base
        async with sem:
            remote_size = await stat_remote(pool.get_sftp(), remote_path)
        if remote_size is not None and remote_size == size:
            _logger.debug("Skipping %s (remote=%d == local=%d)", rel_posix, remote_size, size)
            report("skipped", size, 1)
        else:
            if remote_size is None:
                _logger.debug("Uploading %s (not found on remote)", rel_posix)
            else:
                _logger.debug("Re-uploading %s (remote=%d != local=%d)", rel_posix, remote_size, size)
            await put((local_file, remote_path, size))

    async def _classify_all(put) -> None:
        await asyncio.gather(*[_classify(it, put) for it in items])

    async def _upload(entry: tuple[str, str, int]) -> None:
        local_file, remote_path, size = entry
        await write_file(pool.get_sftp(), local_file, remote_path)
        report("uploaded", size, 1)

    await run_workers(_classify_all, _upload, max_concurrent)


def _shard_main_sync(
    shard: list[tuple[str, str, int]],
    ssh: SSHConfig,
    remote_base: str,
    max_concurrent: int,
    stat_concurrency: int,
    progress_queue,
) -> None:
    """Worker-process entry point: copy *shard* and report deltas upstream.

    Runs its own event loop and :class:`SSHPool` (one process per core), pushing
    batched ``("delta", n_bytes, n_files, is_skip)`` tuples onto *progress_queue*
    and exactly one terminal ``("done",)`` or ``("error", repr)`` before exit.
    """
    # Become a process-group leader so the coordinator can take this worker
    # (and any ssh child it spawns) down as a group when killed, instead of
    # orphaning it.  Harmless if the platform lacks setpgrp.
    if hasattr(os, "setpgrp"):
        try:
            os.setpgrp()
        except OSError:
            pass
    rc = 0
    try:
        asyncio.run(
            _shard_worker(
                shard, ssh, remote_base, max_concurrent, stat_concurrency,
                progress_queue,
            )
        )
        progress_queue.put(("done",))
    except BaseException as exc:  # noqa: BLE001 - report any failure upstream
        progress_queue.put(("error", repr(exc)))
        rc = 1
    finally:
        progress_queue.close()
        progress_queue.join_thread()
    sys.exit(rc)


async def _shard_worker(
    shard: list[tuple[str, str, int]],
    ssh: SSHConfig,
    remote_base: str,
    max_concurrent: int,
    stat_concurrency: int,
    progress_queue,
) -> None:
    """Async body of a worker process; see :func:`_shard_main_sync`."""
    loop = asyncio.get_running_loop()
    pending = {"up_b": 0, "up_f": 0, "sk_b": 0, "sk_f": 0}
    last_flush = loop.time()

    def _flush() -> None:
        if pending["up_f"] or pending["up_b"]:
            progress_queue.put(("delta", pending["up_b"], pending["up_f"], False))
        if pending["sk_f"] or pending["sk_b"]:
            progress_queue.put(("delta", pending["sk_b"], pending["sk_f"], True))
        pending.update(up_b=0, up_f=0, sk_b=0, sk_f=0)

    def _report(kind: str, n_bytes: int, n_files: int) -> None:
        nonlocal last_flush
        if kind == "uploaded":
            pending["up_b"] += n_bytes
            pending["up_f"] += n_files
        else:
            pending["sk_b"] += n_bytes
            pending["sk_f"] += n_files
        # Coalesce updates to ~1/s so the queue is not flooded per file.
        if loop.time() - last_flush >= 1.0:
            _flush()
            last_flush = loop.time()

    async with ssh.pool() as pool:
        await _run_shard(pool, shard, remote_base, _report, max_concurrent, stat_concurrency)
    _flush()


async def _run_multiprocess(
    *,
    items: list[tuple[str, str, int]],
    processes: int,
    ssh: SSHConfig,
    remote_base: str,
    max_concurrent: int,
    stat_concurrency: int,
    advance: Callable[[bool, int, int], None],
    register: Callable[[int], None] | None = None,
    unregister: Callable[[int], None] | None = None,
) -> None:
    """Shard *items* across *processes* worker processes and aggregate progress.

    Each child runs :func:`_shard_main_sync` with its own SSH connection(s) so
    encryption spreads across cores. Progress deltas flow back over a shared
    queue drained by a background thread into *advance*. Surviving children are
    terminated on any exit; a non-zero child raises ``RuntimeError`` so the
    caller treats the whole copy as failed.

    Parameters
    ----------
    items : list of tuple
        ``(local_path, relative_path, size)`` tuples to distribute.
    processes : int
        Number of worker processes to spawn.
    ssh : SSHConfig
        Connection settings; each worker opens its own pool.
    advance : callable
        ``advance(is_skip, n_bytes, n_files)`` applied for each progress delta;
        must be thread-safe (it is called from a background drain thread).
    register : callable, optional
        ``register(pgid)`` called with each spawned worker's process-group id
        so a kill of the coordinator can take the worker group down.
    unregister : callable, optional
        ``unregister(pgid)`` called once a worker has been reaped.
    """
    loop = asyncio.get_running_loop()
    ctx = mp.get_context("spawn")
    progress_queue = ctx.Queue()

    shards = [s for s in _partition_by_bytes(items, processes) if s]
    procs: list = []
    for shard in shards:
        p = ctx.Process(
            target=_shard_main_sync,
            args=(
                shard, ssh, remote_base, max_concurrent, stat_concurrency,
                progress_queue,
            ),
        )
        p.start()
        procs.append(p)
        # The worker makes itself a group leader (see _shard_main_sync), so
        # its PID is also its process-group id; register it so a kill of the
        # coordinator takes the whole worker group down.
        if register is not None and p.pid is not None:
            register(p.pid)

    n_procs = len(procs)
    errors: list[str] = []
    stop_drain = threading.Event()

    def _drain() -> None:
        finished = 0
        while finished < n_procs and not stop_drain.is_set():
            try:
                msg = progress_queue.get(timeout=1.0)
            except _queue.Empty:
                continue
            kind = msg[0]
            if kind == "delta":
                _, n_bytes, n_files, is_skip = msg
                advance(is_skip, n_bytes, n_files)
            elif kind == "done":
                finished += 1
            elif kind == "error":
                errors.append(msg[1])
                finished += 1

    try:
        await loop.run_in_executor(None, _drain)
        for p in procs:
            await loop.run_in_executor(None, p.join)
    finally:
        # Unblock the drain thread when we leave early (cancelled, e.g. by a
        # progress callback that aborted the copy): the children it waits on
        # are about to be terminated and will never report "done".
        stop_drain.set()
        for p in procs:
            if p.is_alive():
                p.terminate()
        for p in procs:
            if p.is_alive():
                await loop.run_in_executor(None, p.join)
        if unregister is not None:
            for p in procs:
                if p.pid is not None:
                    unregister(p.pid)

    if errors:
        raise RuntimeError(
            f"{len(errors)} transfer worker(s) failed: " + "; ".join(errors[:5])
        )
    bad = [p.exitcode for p in procs if p.exitcode not in (0, None)]
    if bad:
        raise RuntimeError(f"{len(bad)} transfer worker(s) exited non-zero: {bad}")


async def _copy_to_ssh(
    source: str,
    target: str,
    broker_url: str | None,
    name: str,
    site: str | None,
    max_concurrent: int,
    ssh_port: int,
    ssh_key: str | None,
    ssh_connections: int,
    track_bytes: bool,
    ssl_verify: bool,
    on_monitor: Callable | None = None,
    quiet: bool = False,
    processes: int = 1,
    stat_concurrency: int = 64,
    encryption_algs: list[str] | None = None,
    steal: bool = False,
    progress_callback: Callable[[int, int, int], None] | None = None,
    progress_interval: float = 5.0,
) -> dict:
    """Copy the local *source* directory to the SSH *target*.

    Parameters
    ----------
    source : str
        Local directory to copy.
    target : str
        Remote target in the form ``[user@]host:/path``.
    broker_url : str or None
        Relay WebSocket URL for monitoring only; ``None`` disables monitoring.
    name : str
        Client name on the relay.
    site : str or None
        Site label for monitor messages.
    max_concurrent : int
        Number of parallel SFTP uploads.
    ssh_port : int
        SSH port on the target host.
    ssh_key : str or None
        Path to the SSH private key file.
    ssh_connections : int
        Number of SSH connections to open in the pool.
    track_bytes : bool
        Show byte-based progress instead of file count.
    ssl_verify : bool
        Verify TLS certificate for the relay connection.
    on_monitor : callable, optional
        Async callback invoked with ``(message, status=..., **kwargs)`` for
        every monitor event, mirroring the hook in
        :func:`nexus_transfers.copy.copy`.  Receives the same structured
        ``progress`` dict the relay does, regardless of whether
        ``broker_url`` is set.
    quiet : bool
        If True, suppress rich console output (monitor events still fire).
    processes : int
        Number of OS worker processes to shard the file set across. ``<= 1``
        keeps the single-process path; ``> 1`` spreads SSH encryption across
        cores (each process opens its own SSH connection(s)).
    stat_concurrency : int
        Maximum concurrent remote ``stat`` calls during the resume scan.
    encryption_algs : list of str or None
        SSH cipher preference list; None uses the GCM-first default in
        :data:`nexus_transfers.ssh.DEFAULT_ENCRYPTION_ALGS`.
    steal : bool
        If True (and ``broker_url`` is set), displace any peer already
        registered under ``name`` before connecting (soft then hard kill), and
        abort if the name cannot be claimed.  With a name keyed on the
        transfer (``nexus-location-<location_uuid>``) this acts as a
        per-transfer interlock so only one push of it runs at a time.  Requires ``broker_url`` — the relay name is the lock.
    progress_callback : callable, optional
        ``progress_callback(bytes_done, bytes_total, files_done)`` called
        every ``progress_interval`` seconds and once at the end, from the
        event-loop thread.  ``bytes_done`` / ``files_done`` include the files
        skipped because they were already at the target.  If it raises, the
        copy is aborted (the workers are stopped) and the exception
        propagates to the caller — this is how a caller stops a transfer it
        no longer owns.
    progress_interval : float
        Seconds between ``progress_callback`` calls.

    Returns
    -------
    dict
        ``{"bytes", "files", "transferred_bytes", "transferred_files",
        "skipped_bytes", "skipped_files"}`` — ``bytes`` / ``files`` are the
        totals of the source (what the target now holds).
    """
    user, host, remote_base = parse_ssh_target(target)
    ssh = SSHConfig(host, user, ssh_port, ssh_key, ssh_connections,
                    encryption_algs)
    source = os.path.expanduser(source)
    console = make_console(quiet=quiet)

    label = os.path.basename(source.rstrip("/")) or source
    dest_label = f"{site}:{remote_base}" if site else f"{host}:{remote_base}"
    console.print(
        f"Copying [yellow]{source}[/yellow] -> [yellow]{dest_label}[/yellow]"
    )

    # When stealing, the relay name is the per-transfer lock: displace any
    # incumbent worker before we register, and treat a lost name race as fatal
    # (continuing unlocked would allow two pushes of the same transfer to run).
    async with await Monitor.connect(
        name, broker_url, ssl_verify=ssl_verify, steal=steal,
        on_monitor=on_monitor,
    ) as monitor:
        await monitor.emit(
            f"{name}: starting copy {source} -> {dest_label}",
            status="progress",
        )

        progress = make_progress(quiet)
        stats = CopyStats(name, label, progress, track_bytes=track_bytes)
        progress.start()
        try:
            items, total_count, total_size = await _list_local(source)
            stats.add_total(total_count, total_size)
            stats.listed()

            async def _copy_body() -> None:
                if processes <= 1:
                    async with ssh.pool() as pool:
                        await _run_shard(
                            pool, items, remote_base,
                            lambda kind, nb, nf: stats.advance(
                                kind == "skipped", nb, nf),
                            max_concurrent, stat_concurrency,
                        )
                else:
                    client = monitor.client
                    await _run_multiprocess(
                        items=items,
                        processes=processes,
                        ssh=ssh,
                        remote_base=remote_base,
                        max_concurrent=max_concurrent,
                        stat_concurrency=stat_concurrency,
                        advance=stats.advance,
                        register=client.register_child_pgid if client else None,
                        unregister=(
                            client.unregister_child_pgid if client else None
                        ),
                    )

            async with ticking(30, lambda: stats.heartbeat(monitor)):
                await run_abortable(
                    _copy_body(), stats, progress_callback, progress_interval,
                )
        finally:
            progress.stop()

        await stats.finish(monitor, console)
    return stats.result()


def main() -> None:
    """CLI entry point for ``nexus-transfers copy-ssh``."""
    parser = CommandParser(
        "copy_ssh",
        description="Copy a local directory to a remote SSH/SFTP target",
    )
    parser.add_argument("--source", required=True, help="Local directory to copy")
    parser.add_argument(
        "--target", required=True,
        help="Remote target: [user@]host:/remote/path",
    )
    parser.monitor_options()
    parser.option("--max-concurrent", type=int, default=4,
                  help="Number of parallel SFTP uploads (default: 4)")
    parser.ssh_options("Number of SSH connections to open per process "
                       "(default: 2)")
    parser.option("--processes", type=int, default=1,
                  help="Number of worker processes to shard files across; >1 "
                       "spreads SSH encryption over cores (default: 1)")
    parser.option("--stat-concurrency", type=int, default=64,
                  help="Max concurrent remote stat calls during the resume "
                       "scan (default: 64)")
    parser.option("--size", action="store_true",
                  help="Show byte-based progress instead of file count")
    parser.option(
        "--steal", action="store_true",
        help="If a client is already registered under --name, kill it (soft "
             "kill first, then hard kill if it does not exit) and take over "
             "the name, aborting if the name cannot be claimed. With a name "
             "keyed on the transfer (e.g. nexus-location-<location_uuid>) this "
             "guarantees only one push of it runs at a time. Requires "
             "--broker-url.",
    )
    parser.debug_option()
    args = parser.parse_args()

    setup_cli_logging(debug=args.debug)

    tag = args.site or "ssh-copy"
    name = args.name or f"{tag}-{uuid.uuid4().hex[:8]}"

    asyncio.run(
        _copy_to_ssh(
            source=args.source,
            target=args.target,
            broker_url=args.broker_url,
            name=name,
            site=args.site,
            max_concurrent=args.max_concurrent,
            ssh_port=args.ssh_port,
            ssh_key=args.ssh_key,
            ssh_connections=args.ssh_connections,
            track_bytes=args.size,
            ssl_verify=not args.no_verify,
            processes=args.processes,
            stat_concurrency=args.stat_concurrency,
            encryption_algs=args.cipher,
            steal=args.steal,
        )
    )


if __name__ == "__main__":
    main()
