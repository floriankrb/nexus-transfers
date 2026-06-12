"""CLI tool: copy a local directory to a remote SSH/SFTP target.

Usage::

    nexus-copy-to-ssh --source /data/dataset.zarr --target user@host:/remote/path
"""

import argparse
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

from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
)

from nexus_transfers._progress import (
    _BinarySpeedColumn,
    _CountOrBytesColumn,
    _fmt_binary,
    make_console,
    setup_cli_logging,
)
from nexus_transfers.client import Client
from nexus_transfers.config import cli_default
from nexus_transfers.ssh import SSHPool, stat_remote, write_file

_logger = logging.getLogger(__name__)


def _parse_target(target: str) -> tuple[str | None, str, str]:
    """Parse ``[user@]host:/path`` into ``(user, host, remote_path)``.

    Parameters
    ----------
    target : str
        Target specification in the form ``[user@]host:/path``.

    Raises
    ------
    ValueError
        If *target* does not contain a colon separator.
    """
    if ":" not in target:
        raise ValueError(
            f"Invalid target {target!r}: expected [user@]host:/path"
        )
    host_part, remote_path = target.split(":", 1)
    if "@" in host_part:
        user, host = host_part.split("@", 1)
    else:
        user, host = None, host_part
    return user, host, remote_path


async def _list_local(source_path: str) -> tuple[list[tuple[str, str, int]], int, int]:
    """Walk *source_path* and return ``(items, total_count, total_size)``.

    Each item is a ``(local_path, relative_path, size)`` tuple. Runs
    ``os.scandir`` in a thread-pool executor so NFS stat calls do not block the
    event loop.

    Parameters
    ----------
    source_path : str
        Root directory to walk.
    """
    loop = asyncio.get_running_loop()

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
    upload_queue: asyncio.Queue = asyncio.Queue()
    sem = asyncio.Semaphore(stat_concurrency)

    async def _classify(item: tuple[str, str, int]) -> None:
        local_file, rel_path, size = item
        rel_posix = PurePosixPath(rel_path).as_posix()
        remote_path = f"{remote_base}/{rel_posix}"
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
            await upload_queue.put((local_file, remote_path, size))

    async def _classify_all() -> None:
        await asyncio.gather(*[_classify(it) for it in items])
        for _ in range(max_concurrent):
            await upload_queue.put(None)

    async def _upload_worker() -> None:
        sftp = pool.get_sftp()
        while True:
            entry = await upload_queue.get()
            if entry is None:
                return
            local_file, remote_path, size = entry
            await write_file(sftp, local_file, remote_path)
            report("uploaded", size, 1)

    await asyncio.gather(
        _classify_all(),
        *[_upload_worker() for _ in range(max_concurrent)],
    )


def _shard_main_sync(
    shard: list[tuple[str, str, int]],
    host: str,
    port: int,
    user: str | None,
    key_path: str | None,
    ssh_connections: int,
    remote_base: str,
    encryption_algs: list[str] | None,
    max_concurrent: int,
    stat_concurrency: int,
    progress_queue,
) -> None:
    """Worker-process entry point: copy *shard* and report deltas upstream.

    Runs its own event loop and :class:`SSHPool` (one process per core), pushing
    batched ``("delta", n_bytes, n_files, is_skip)`` tuples onto *progress_queue*
    and exactly one terminal ``("done",)`` or ``("error", repr)`` before exit.
    """
    rc = 0
    try:
        asyncio.run(
            _shard_worker(
                shard, host, port, user, key_path, ssh_connections,
                remote_base, encryption_algs, max_concurrent, stat_concurrency,
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
    host: str,
    port: int,
    user: str | None,
    key_path: str | None,
    ssh_connections: int,
    remote_base: str,
    encryption_algs: list[str] | None,
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

    async with SSHPool(
        host, port, user, key_path, ssh_connections, encryption_algs,
    ) as pool:
        await _run_shard(pool, shard, remote_base, _report, max_concurrent, stat_concurrency)
    _flush()


async def _run_multiprocess(
    *,
    items: list[tuple[str, str, int]],
    processes: int,
    host: str,
    ssh_port: int,
    user: str | None,
    ssh_key: str | None,
    ssh_connections: int,
    remote_base: str,
    encryption_algs: list[str] | None,
    max_concurrent: int,
    stat_concurrency: int,
    advance: Callable[[bool, int, int], None],
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
    advance : callable
        ``advance(is_skip, n_bytes, n_files)`` applied for each progress delta;
        must be thread-safe (it is called from a background drain thread).
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
                shard, host, ssh_port, user, ssh_key, ssh_connections,
                remote_base, encryption_algs, max_concurrent, stat_concurrency,
                progress_queue,
            ),
        )
        p.start()
        procs.append(p)

    n_procs = len(procs)
    errors: list[str] = []

    def _drain() -> None:
        finished = 0
        while finished < n_procs:
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
        for p in procs:
            if p.is_alive():
                p.terminate()
        for p in procs:
            if p.is_alive():
                await loop.run_in_executor(None, p.join)

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
) -> None:
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
    """
    user, host, remote_base = _parse_target(target)
    source = os.path.expanduser(source)
    console = make_console(quiet=quiet)

    label = os.path.basename(source.rstrip("/")) or source
    dest_label = f"{site}:{remote_base}" if site else f"{host}:{remote_base}"
    if not quiet:
        console.print(
            f"Copying [yellow]{source}[/yellow] -> [yellow]{dest_label}[/yellow]"
        )

    monitor_client: Client | None = None
    if broker_url:
        try:
            # Monitoring is best-effort: keep retrying forever so a dropped
            # relay connection (e.g. a keepalive timeout) silently reconnects
            # instead of permanently losing live progress for the rest of the
            # copy.
            monitor_client = Client(
                name, broker_url, dispatch={},
                ssl_verify=ssl_verify, reconnect_retries=-1,
            )
            await monitor_client.connect()
        except Exception as exc:
            _logger.warning(
                "Relay unavailable (%s), continuing without monitor", exc,
            )
            monitor_client = None

    async def _emit(message, status=None, **kw):
        if monitor_client is not None:
            try:
                await monitor_client.monitor(message, status=status, **kw)
            except Exception as exc:
                _logger.warning("Failed to send monitor event: %s", exc)
        if on_monitor is not None:
            try:
                await on_monitor(message, status=status, **kw)
            except Exception as exc:
                _logger.warning("on_monitor callback failed: %s", exc)

    await _emit(
        f"{name}: starting copy {source} -> {dest_label}",
        status="progress",
    )

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        _CountOrBytesColumn(),
        _BinarySpeedColumn(),
        TimeRemainingColumn(),
        transient=True,
    )

    loop = asyncio.get_running_loop()
    start = loop.time()
    total_bytes = 0
    done_count = 0
    skipped = 0
    skipped_bytes = 0
    lock = threading.Lock()

    unit = "bytes" if track_bytes else "files"
    walk_task_id = progress.add_task(
        f"[magenta]Listing {label}[/magenta]", total=None, unit="files",
    )
    copy_task_id = progress.add_task(
        f"[cyan]Copying {label}[/cyan]", total=None, unit=unit,
    )
    progress.start()

    items, total_count, total_size = await _list_local(source)
    progress.update(copy_task_id, total=total_size if track_bytes else total_count)
    progress.remove_task(walk_task_id)

    def _advance(is_skip: bool, n_bytes: int, n_files: int) -> None:
        """Update shared counters and the rich bar (thread-safe)."""
        nonlocal total_bytes, done_count, skipped, skipped_bytes
        with lock:
            if is_skip:
                skipped += n_files
                skipped_bytes += n_bytes
            else:
                total_bytes += n_bytes
                done_count += n_files
            if track_bytes:
                progress.advance(copy_task_id, n_bytes)
            else:
                progress.update(copy_task_id, completed=done_count + skipped)
            if skipped:
                progress.update(
                    copy_task_id,
                    description=(
                        f"[cyan]Copying {label}[/cyan] "
                        f"[dim]({skipped} skipped)[/dim]"
                    ),
                )

    def _payload() -> tuple[float, dict]:
        with lock:
            elapsed = loop.time() - start
            rate = total_bytes / elapsed if elapsed > 0 else 0
            return rate, {
                "label": f"{name}: {done_count} files",
                "value": total_bytes + skipped_bytes,
                "maximum": total_size or None,
                "unit": "byte",
                "total_transferred": total_bytes + skipped_bytes,
                "files_done": done_count,
                "files_skipped": skipped,
                "rate": rate,
            }

    async def _ticker(stop_event: asyncio.Event) -> None:
        """Emit aggregated progress every 30s until *stop_event* is set."""
        while True:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=30)
                return
            except asyncio.TimeoutError:
                pass
            rate, payload = _payload()
            skip_suffix = (
                f" [{skipped} skipped, {_fmt_binary(skipped_bytes)}]"
                if skipped else ""
            )
            await _emit(
                f"{name}: {done_count} files "
                f"({_fmt_binary(total_bytes)}, {_fmt_binary(rate)}/s)" + skip_suffix,
                status="progress",
                progress=payload,
            )

    stop_event = asyncio.Event()
    ticker = asyncio.create_task(_ticker(stop_event))

    try:
        if processes <= 1:
            async with SSHPool(
                host, ssh_port, user, ssh_key, ssh_connections, encryption_algs,
            ) as pool:
                await _run_shard(
                    pool, items, remote_base,
                    lambda kind, nb, nf: _advance(kind == "skipped", nb, nf),
                    max_concurrent, stat_concurrency,
                )
        else:
            await _run_multiprocess(
                items=items,
                processes=processes,
                host=host,
                ssh_port=ssh_port,
                user=user,
                ssh_key=ssh_key,
                ssh_connections=ssh_connections,
                remote_base=remote_base,
                encryption_algs=encryption_algs,
                max_concurrent=max_concurrent,
                stat_concurrency=stat_concurrency,
                advance=_advance,
            )
    finally:
        stop_event.set()
        await ticker
        progress.stop()

    if skipped:
        _logger.info(
            "Skipped %d already-complete file(s) (%s)",
            skipped, _fmt_binary(skipped_bytes),
        )
        if not quiet:
            console.print(
                f"Skipped [bold]{skipped}[/bold] already-complete file(s) "
                f"([bold]{_fmt_binary(skipped_bytes)}[/bold])"
            )

    elapsed = loop.time() - start
    rate = total_bytes / elapsed if elapsed > 0 else 0
    skip_suffix = (
        f" [{skipped} skipped, {_fmt_binary(skipped_bytes)}]"
        if skipped else ""
    )
    summary = (
        f"Transferred {_fmt_binary(total_bytes)} "
        f"in {elapsed:.1f}s ({_fmt_binary(rate)}/s)" + skip_suffix
    )
    if not quiet:
        console.print(
            f"Transferred [bold]{_fmt_binary(total_bytes)}[/bold] "
            f"in [bold]{elapsed:.1f}s[/bold] "
            f"([bold]{_fmt_binary(rate)}/s[/bold])"
        )

    await _emit(
        f"{name}: {summary}",
        status="ok",
        progress={
            "label": f"{name}: {done_count} files",
            "value": total_bytes + skipped_bytes,
            "maximum": total_size or None,
            "unit": "byte",
            "total_transferred": total_bytes + skipped_bytes,
            "files_done": done_count,
            "files_skipped": skipped,
            "rate": rate,
        },
    )

    if monitor_client:
        await monitor_client.close()


def main() -> None:
    """CLI entry point for ``nexus-copy-to-ssh``."""
    parser = argparse.ArgumentParser(
        description="Copy a local directory to a remote SSH/SFTP target",
    )
    parser.add_argument("--source", required=True, help="Local directory to copy")
    parser.add_argument(
        "--target", required=True,
        help="Remote target: [user@]host:/remote/path",
    )
    parser.add_argument(
        "--broker-url",
        default=cli_default("broker_url", "copy_ssh", default=None),
        help="Relay WebSocket URL for monitoring (default: none — monitoring disabled)",
    )
    parser.add_argument(
        "--name", default=cli_default("name", "copy_ssh", default=None),
        help="Client name on the relay (default: auto-generated)",
    )
    parser.add_argument(
        "--site",
        default=cli_default("site", "copy_ssh", default=None),
        help="Site label for monitor messages",
    )
    parser.add_argument(
        "--max-concurrent", type=int,
        default=cli_default("max_concurrent", "copy_ssh", default=4, type_fn=int),
        help="Number of parallel SFTP uploads (default: 4)",
    )
    parser.add_argument(
        "--ssh-port", type=int,
        default=cli_default("ssh_port", "copy_ssh", default=22, type_fn=int),
        help="SSH port (default: 22)",
    )
    parser.add_argument(
        "--ssh-key",
        default=cli_default("ssh_key", "copy_ssh", default=None),
        help="Path to SSH private key",
    )
    parser.add_argument(
        "--ssh-connections", type=int,
        default=cli_default("ssh_connections", "copy_ssh", default=2, type_fn=int),
        help="Number of SSH connections to open per process (default: 2)",
    )
    parser.add_argument(
        "--processes", type=int,
        default=cli_default("processes", "copy_ssh", default=1, type_fn=int),
        help="Number of worker processes to shard files across; >1 spreads SSH "
             "encryption over cores (default: 1)",
    )
    parser.add_argument(
        "--stat-concurrency", type=int,
        default=cli_default("stat_concurrency", "copy_ssh", default=64, type_fn=int),
        help="Max concurrent remote stat calls during the resume scan (default: 64)",
    )
    parser.add_argument(
        "--cipher", nargs="+", default=None, metavar="ALG",
        help="SSH cipher preference list (default: aes128-gcm@openssh.com first)",
    )
    parser.add_argument(
        "--size", action="store_true",
        default=cli_default("size", "copy_ssh", default=False),
        help="Show byte-based progress instead of file count",
    )
    parser.add_argument(
        "--no-verify", action="store_true",
        default=cli_default("no_verify", "copy_ssh", default=False),
        help="Skip TLS verification for the relay connection",
    )
    parser.add_argument("--debug", action="store_true",
                        default=cli_default("debug", "copy_ssh", default=False),
                        help="Enable debug logging")
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
        )
    )


if __name__ == "__main__":
    main()
