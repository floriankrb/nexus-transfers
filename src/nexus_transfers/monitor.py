"""CLI tool that registers as the ``monitor`` peer and prints messages.

Usage::

    nexus-monitor --server-url wss://example.com/transfers
"""

import argparse
import asyncio
import logging
from datetime import datetime, timezone

from nexus_transfers.client import Client, _DEFAULT_URL
from nexus_transfers.dispatch import DISPATCH
from rich.console import Console

_STATUS_STYLES = {
    "ok": "bold green",
    "error": "bold red",
    "progress": "bold cyan",
    "warning": "bold yellow",
}


def main():
    """CLI entry point for ``nexus-monitor``."""
    parser = argparse.ArgumentParser(
        description="Monitor peer – prints monitoring messages from other clients",
    )
    parser.add_argument(
        "--server-url",
        default=None,
        help=f"Server WebSocket URL (default: {_DEFAULT_URL})",
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

    asyncio.run(
        _run_monitor(
            url=args.server_url,
            reconnect_retries=args.reconnect_retries,
            reconnect_delay=args.reconnect_delay,
            ssl_verify=not args.no_verify,
        )
    )


async def _run_monitor(url, **client_kwargs):
    """Register as ``monitor`` and print incoming log messages."""
    console = Console()

    dispatch = dict(DISPATCH)

    def log(message: str, status: str | None = None):
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        style = _STATUS_STYLES.get(status, "")
        status_tag = f" [{style}]\\[{status}][/{style}]" if status else ""
        console.print(f"[dim]{ts}[/dim]{status_tag} {message}")
        return True

    dispatch["log"] = log

    async with Client("monitor", url, dispatch=dispatch, **client_kwargs) as client:
        console.print(
            f"[bold green]Monitor[/bold green] connected to "
            f"[cyan]{client.url}[/cyan]"
        )
        console.print("[dim]Waiting for messages… (Ctrl+C to quit)[/dim]")
        try:
            await client._listener_task
        except asyncio.CancelledError:
            pass
    console.print("[dim]Disconnected.[/dim]")


if __name__ == "__main__":
    main()
