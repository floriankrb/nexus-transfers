"""CLI tool: copy a local directory to a remote SSH/SFTP target.

Usage::

    nexus-copy-to-ssh --source /data/dataset.zarr --target user@host:/remote/path
"""

import argparse
import asyncio
import logging
import os
import uuid
from pathlib import PurePosixPath

from rich.console import Console
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
)
from nexus_transfers.client import _DEFAULT_URL, Client
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


async def _walk_local(source_path: str, queue: asyncio.Queue) -> None:
    """Walk *source_path* and push ``(local_file, rel_path, size)`` onto *queue*.

    Runs ``os.scandir`` in a thread-pool executor so NFS stat calls do not
    block the event loop.

    Parameters
    ----------
    source_path : str
        Root directory to walk.
    queue : asyncio.Queue
        Destination queue for ``(local_path, relative_path, size)`` tuples.
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
    for item in items:
        await queue.put(item)


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
    """
    user, host, remote_base = _parse_target(target)
    source = os.path.expanduser(source)
    console = Console()

    label = os.path.basename(source.rstrip("/")) or source
    dest_label = f"{site}:{remote_base}" if site else f"{host}:{remote_base}"
    console.print(
        f"Copying [yellow]{source}[/yellow] -> [yellow]{dest_label}[/yellow]"
    )

    monitor_client: Client | None = None
    if broker_url:
        try:
            monitor_client = Client(
                name, broker_url, dispatch={},
                ssl_verify=ssl_verify, reconnect_retries=0,
            )
            await monitor_client.connect()
            await monitor_client.monitor(
                f"{name}: starting copy {source} -> {dest_label}",
                status="progress",
            )
        except Exception as exc:
            _logger.warning(
                "Relay unavailable (%s), continuing without monitor", exc,
            )
            monitor_client = None

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
    last_monitor_time = start
    total_bytes = 0
    done_count = 0
    skipped = 0
    skipped_bytes = 0

    async with SSHPool(host, ssh_port, user, ssh_key, ssh_connections) as pool:
        queue: asyncio.Queue = asyncio.Queue()

        unit = "bytes" if track_bytes else "files"
        walk_task_id = progress.add_task(
            f"[magenta]Listing {label}[/magenta]", total=None, unit="files",
        )
        copy_task_id = progress.add_task(
            f"[cyan]Copying {label}[/cyan]", total=None, unit=unit,
        )
        progress.start()

        async def _walk_and_enqueue() -> None:
            await _walk_local(source, queue)
            for _ in range(max_concurrent):
                await queue.put(None)
            progress.remove_task(walk_task_id)

        async def _worker() -> None:
            nonlocal total_bytes, done_count, last_monitor_time, skipped, skipped_bytes
            sftp = pool.get_sftp()
            while True:
                item = await queue.get()
                if item is None:
                    return
                local_file, rel_path, size = item
                rel_posix = PurePosixPath(rel_path).as_posix()
                remote_path = f"{remote_base}/{rel_posix}"

                # Resume: skip if remote size already matches local size.
                remote_size = await stat_remote(sftp, remote_path)
                if remote_size is not None and remote_size == size:
                    skipped += 1
                    skipped_bytes += size
                    continue

                await write_file(sftp, local_file, remote_path)
                total_bytes += size
                done_count += 1

                if track_bytes:
                    progress.advance(copy_task_id, size)
                else:
                    progress.update(copy_task_id, completed=done_count)

                now = loop.time()
                if monitor_client and now - last_monitor_time >= 30:
                    last_monitor_time = now
                    elapsed = now - start
                    rate = total_bytes / elapsed if elapsed > 0 else 0
                    await monitor_client.monitor(
                        f"{name}: {done_count} files "
                        f"({_fmt_binary(total_bytes)}, {_fmt_binary(rate)}/s)",
                        status="progress",
                    )

        await asyncio.gather(
            _walk_and_enqueue(),
            *[_worker() for _ in range(max_concurrent)],
        )
        progress.stop()

    if skipped:
        _logger.info(
            "Skipped %d already-complete file(s) (%s)",
            skipped, _fmt_binary(skipped_bytes),
        )
        console.print(
            f"Skipped [bold]{skipped}[/bold] already-complete file(s) "
            f"([bold]{_fmt_binary(skipped_bytes)}[/bold])"
        )

    elapsed = loop.time() - start
    rate = total_bytes / elapsed if elapsed > 0 else 0
    summary = (
        f"Transferred {_fmt_binary(total_bytes)} "
        f"in {elapsed:.1f}s ({_fmt_binary(rate)}/s)"
    )
    console.print(
        f"Transferred [bold]{_fmt_binary(total_bytes)}[/bold] "
        f"in [bold]{elapsed:.1f}s[/bold] "
        f"([bold]{_fmt_binary(rate)}/s[/bold])"
    )

    if monitor_client:
        await monitor_client.monitor(f"{name}: {summary}", status="ok")
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
        help=f"Relay WebSocket URL for monitoring (default: ${_DEFAULT_URL!r})",
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
        help="Number of SSH connections to open (default: 2)",
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

    from rich.logging import RichHandler
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        handlers=[RichHandler(rich_tracebacks=True)],
    )

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
        )
    )


if __name__ == "__main__":
    main()
