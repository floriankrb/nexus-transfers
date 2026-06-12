"""CLI tool for recursive remote-to-local directory copy.

Usage::

    nexus-copy --from a /remote/dir /local/dir
    nexus-copy --from a src ./mirror --broker-url wss://example.com/transfers
"""

import argparse
import asyncio
import datetime
import os
import uuid


from nexus_transfers._progress import make_console, setup_cli_logging
from nexus_transfers.client import _DEFAULT_URL, Client
from nexus_transfers.config import cli_default


async def list_dir(name, broker_url, remote_client, path, **client_kwargs):
    """List the contents of a remote directory, handling pagination.

    Parameters
    ----------
    name:
        Local nexus client name for this session.
    broker_url:
        WebSocket broker URL.
    remote_client:
        Name of the remote nexus client.
    path:
        Path on the remote client to list.
    **client_kwargs:
        Forwarded to :class:`~nexus_transfers.client.Client`.

    Returns
    -------
    list[dict]
        All entries from the remote directory.  Each entry has at minimum
        ``"name"`` and ``"type"`` keys; ``"size"`` is included for files
        when the remote side supports it.
    """
    async with Client(name, broker_url, **client_kwargs) as client:
        entries = []
        offset = 0
        limit = 1000
        while True:
            page = await client.send(
                f"{remote_client}.list_dir", path,
                offset=offset, limit=limit,
            )
            entries.extend(page)
            if len(page) < limit:
                break
            offset += len(page)
        return entries



def main():
    """CLI entry point for ``nexus-copy``."""
    parser = argparse.ArgumentParser(
        description="Recursively copy a directory from a remote nexus client",
    )
    parser.add_argument(
        "--from",
        dest="remote_client",
        required=True,
        help="Name of the remote client to copy from",
    )
    parser.add_argument("source", help="Remote directory path")
    parser.add_argument("target", help="Local destination directory")
    parser.add_argument(
        "--broker-url",
        default=cli_default("broker_url", "copy", default=None),
        help=f"Broker WebSocket URL (default: {_DEFAULT_URL})",
    )
    parser.add_argument(
        "--name",
        default=cli_default("name", "copy", default=None),
        help="Client name (default: auto-generated)",
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=cli_default("max_concurrent", "copy", default=4, type_fn=int),
        help="Maximum parallel file transfers (default: 4)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=cli_default("chunk_size", "copy", default=65536, type_fn=int),
        help="Binary chunk size in bytes for file transfers (default: 65536)",
    )
    parser.add_argument(
        "--use-broker",
        action="store_true",
        default=cli_default("use_broker", "copy", default=False),
        help="Transfer via the WebSocket relay instead of S3 staging",
    )
    parser.add_argument(
        "--reconnect-retries",
        type=int,
        default=cli_default("reconnect_retries", "copy", default=-1, type_fn=int),
        help="Reconnection attempts on disconnect (-1 = infinite, default: -1)",
    )
    parser.add_argument(
        "--reconnect-delay",
        type=float,
        default=cli_default("reconnect_delay", "copy", default=2.0, type_fn=float),
        help="Seconds between reconnection attempts (default: 2.0)",
    )
    parser.add_argument(
        "--peer-retries",
        type=int,
        default=cli_default("peer_retries", "copy", default=-1, type_fn=int),
        help="Retries when target peer is not found (-1 = infinite, default: -1)",
    )
    parser.add_argument(
        "--peer-delay",
        type=float,
        default=cli_default("peer_delay", "copy", default=2.0, type_fn=float),
        help="Seconds between peer-not-found retries (default: 2.0)",
    )
    parser.add_argument(
        "--call-timeout",
        type=float,
        default=cli_default("call_timeout", "copy", default=None, type_fn=float),
        help="Timeout in seconds for RPC calls (default: no timeout)",
    )
    parser.add_argument(
        "--no-verify",
        action="store_true",
        default=cli_default("no_verify", "copy", default=False),
        help="Skip TLS certificate verification for wss:// connections",
    )
    parser.add_argument(
        "--site",
        default=cli_default("site", "copy", default=None),
        help="Site label used in the auto-generated client name instead of 'copy'",
    )
    parser.add_argument(
        "--size",
        action="store_true",
        default=cli_default("size", "copy", default=False),
        help="Show transfer progress in bytes and use size to verify resume skips",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=cli_default("debug", "copy", default=False),
        help="Enable debug logging",
    )
    args = parser.parse_args()

    setup_cli_logging(debug=args.debug)

    tag = args.site or "copy"
    name = args.name or f"{tag}-{uuid.uuid4().hex[:8]}"

    asyncio.run(
        copy(
            name=name,
            broker_url=args.broker_url,
            remote_client=args.remote_client,
            source=args.source,
            target=args.target,
            site=args.site,
            max_concurrent=args.max_concurrent,
            chunk_size=args.chunk_size,
            use_s3=not args.use_broker,
            track_bytes=args.size,
            reconnect_retries=args.reconnect_retries,
            reconnect_delay=args.reconnect_delay,
            peer_retries=args.peer_retries,
            peer_delay=args.peer_delay,
            call_timeout=args.call_timeout,
            ssl_verify=not args.no_verify,
        )
    )


async def copy(name, broker_url, remote_client, source, target, site=None,
               max_concurrent=4, chunk_size=65536, use_s3=True,
               track_bytes=False, quiet=False, on_monitor=None, **client_kwargs):
    """Connect to the relay and copy a remote directory.

    Parameters
    ----------
    name:
        Local nexus client name for this session.
    broker_url:
        WebSocket broker URL.
    remote_client:
        Name of the remote nexus client to copy from.
    source:
        Path on the remote client to copy.
    target:
        Local destination path.
    site:
        Optional site label used in console output and monitor messages.
    max_concurrent:
        Maximum number of parallel file transfers.
    chunk_size:
        Binary chunk size in bytes (only used when ``use_s3`` is False).
    use_s3:
        If True (default), stage transfers through S3.
    track_bytes:
        If True, show progress in bytes and use size to verify resume skips.
    quiet:
        If True, suppress rich console output (monitor events are still emitted).
    on_monitor:
        Optional async callable invoked after each ``client.monitor`` call with
        the same ``(message, status=..., **kwargs)`` signature.  Use this to
        forward progress events to an external system without reimplementing
        the copy loop.
    **client_kwargs:
        Forwarded to :class:`~nexus_transfers.client.Client`.
    """
    target = os.path.expanduser(target)
    console = make_console()
    async with Client(name, broker_url, **client_kwargs) as client:
        if on_monitor is not None:
            _original_monitor = client.monitor

            async def _hooked_monitor(message, status=None, **kw):
                await _original_monitor(message, status=status, **kw)
                await on_monitor(message, status=status, **kw)

            client.monitor = _hooked_monitor
        if not quiet:
            console.print(f"[bold green]Connected[/bold green] to [cyan]{client.url}[/cyan] as '[magenta]{name}[/magenta]'")
        via = "" if use_s3 else " [dim](via broker)[/dim]"
        dest_label = f"{site}:{target}" if site else target
        if not quiet:
            console.print(f"Copying [yellow]{remote_client}:{source}[/yellow] -> [yellow]{dest_label}[/yellow]{via}")
        await client.monitor(
            f"{name}: starting copy {remote_client}:{source} -> {dest_label}",
            status="progress",
        )
        s3_prefix = None
        if use_s3:
            ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d-%H%M%S")
            s3_prefix = f"{ts}-{remote_client}-{name}-{uuid.uuid4()}"
        await client.get_directory(remote_client, source, target,
                                   max_concurrent=max_concurrent,
                                   chunk_size=chunk_size,
                                   use_s3=use_s3,
                                   s3_prefix=s3_prefix,
                                   track_bytes=track_bytes)
        await client.monitor(
            f"{name}: copy complete {remote_client}:{source} -> {dest_label}",
            status="ok",
        )
        if not quiet:
            console.print("[bold green]Done.[/bold green]")

if __name__ == "__main__":
    main()
