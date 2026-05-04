"""CLI tool for recursive remote-to-local directory copy.

Usage::

    nexus-copy --from a /remote/dir /local/dir
    nexus-copy --from a src ./mirror --server-url wss://example.com/transfers
"""

import argparse
import asyncio
import logging
import os
import uuid

from nexus_transfers.client import Client, _DEFAULT_URL
from rich.console import Console

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
        "--server-url",
        default=None,
        help=f"Server WebSocket URL (default: {_DEFAULT_URL})",
    )
    parser.add_argument(
        "--name",
        default=None,
        help="Client name (default: auto-generated)",
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=4,
        help="Maximum parallel file transfers (default: 4)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=65536,
        help="Binary chunk size in bytes for file transfers (default: 65536)",
    )
    parser.add_argument(
        "--use-server",
        action="store_true",
        help="Transfer via the WebSocket relay instead of S3 staging",
    )
    parser.add_argument(
        "--reconnect-retries",
        type=int,
        default=-1,
        help="Reconnection attempts on disconnect (-1 = infinite, default: -1)",
    )
    parser.add_argument(
        "--reconnect-delay",
        type=float,
        default=2.0,
        help="Seconds between reconnection attempts (default: 2.0)",
    )
    parser.add_argument(
        "--peer-retries",
        type=int,
        default=-1,
        help="Retries when target peer is not found (-1 = infinite, default: -1)",
    )
    parser.add_argument(
        "--peer-delay",
        type=float,
        default=2.0,
        help="Seconds between peer-not-found retries (default: 2.0)",
    )
    parser.add_argument(
        "--call-timeout",
        type=float,
        default=None,
        help="Timeout in seconds for RPC calls (default: no timeout)",
    )
    parser.add_argument(
        "--no-verify",
        action="store_true",
        help="Skip TLS certificate verification for wss:// connections",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    from rich.logging import RichHandler
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        handlers=[RichHandler(rich_tracebacks=True)],
    )

    name = args.name or f"copy-{uuid.uuid4().hex[:8]}"

    asyncio.run(
        _copy(
            name=name,
            url=args.server_url,
            remote_client=args.remote_client,
            source=args.source,
            target=args.target,
            max_concurrent=args.max_concurrent,
            chunk_size=args.chunk_size,
            use_s3=not args.use_server,
            reconnect_retries=args.reconnect_retries,
            reconnect_delay=args.reconnect_delay,
            peer_retries=args.peer_retries,
            peer_delay=args.peer_delay,
            call_timeout=args.call_timeout,
            ssl_verify=not args.no_verify,
        )
    )


async def _copy(name, url, remote_client, source, target, max_concurrent,
                chunk_size, use_s3=True, **client_kwargs):
    """Connect to the relay and copy a remote directory."""
    target = os.path.expanduser(target)
    console = Console()
    async with Client(name, url, **client_kwargs) as client:
        console.print(f"[bold green]Connected[/bold green] to [cyan]{url}[/cyan] as '[magenta]{name}[/magenta]'")
        via = "" if use_s3 else " [dim](via server)[/dim]"
        console.print(f"Copying [yellow]{remote_client}:{source}[/yellow] -> [yellow]{target}[/yellow]{via}")
        await client.monitor(
            f"{name}: starting copy {remote_client}:{source} -> {target}",
            status="progress",
        )
        await client.get_directory(remote_client, source, target,
                                   max_concurrent=max_concurrent,
                                   chunk_size=chunk_size,
                                   use_s3=use_s3)
        await client.monitor(
            f"{name}: copy complete {remote_client}:{source} -> {target}",
            status="ok",
        )
        console.print("[bold green]Done.[/bold green]")

if __name__ == "__main__":
    main()
