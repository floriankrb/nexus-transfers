"""CLI tool for recursive remote-to-local directory copy.

Usage::

    nexus-copy --from a /remote/dir /local/dir
    nexus-copy --from a src ./mirror --broker-url wss://example.com/transfers
"""

import argparse
import asyncio
import datetime
import logging
import os
import uuid

from rich.console import Console

from nexus_transfers.client import _DEFAULT_URL, Client
from nexus_transfers.config import cli_default


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
        help="Show transfer progress in bytes instead of file count",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=cli_default("debug", "copy", default=False),
        help="Enable debug logging",
    )
    args = parser.parse_args()

    from rich.logging import RichHandler
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        handlers=[RichHandler(rich_tracebacks=True)],
    )

    tag = args.site or "copy"
    name = args.name or f"{tag}-{uuid.uuid4().hex[:8]}"

    asyncio.run(
        _copy(
            name=name,
            url=args.broker_url,
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


async def _copy(name, url, remote_client, source, target, site=None,
                max_concurrent=4, chunk_size=65536, use_s3=True,
                track_bytes=False, **client_kwargs):
    """Connect to the relay and copy a remote directory."""
    target = os.path.expanduser(target)
    console = Console()
    async with Client(name, url, **client_kwargs) as client:
        console.print(f"[bold green]Connected[/bold green] to [cyan]{url}[/cyan] as '[magenta]{name}[/magenta]'")
        via = "" if use_s3 else " [dim](via broker)[/dim]"
        dest_label = f"{site}:{target}" if site else target
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
        console.print("[bold green]Done.[/bold green]")

if __name__ == "__main__":
    main()
