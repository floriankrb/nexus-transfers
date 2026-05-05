"""CLI tool that registers as a monitoring service and prints broadcast events.

Usage::

    nexus-monitor --broker-url wss://example.com/transfers
"""

import argparse
import asyncio
import logging
import uuid
from datetime import datetime

from rich.console import Console

from nexus_transfers.client import _DEFAULT_URL, Client
from nexus_transfers.config import cli_default

_TYPE_STYLES = {
    "ok": "bold green",
    "connected": "bold green",
    "disconnected": "bold red",
    "error": "bold red",
    "progress": "bold cyan",
    "warning": "bold yellow",
    "info": "cyan",
}


def _format_progress(progress: dict | None) -> str:
    """Format a progress dict into a compact string."""
    if not progress:
        return ""
    label = progress.get("label", "")
    value = progress.get("value")
    maximum = progress.get("maximum")
    unit = progress.get("unit", "")
    rate = progress.get("rate")

    parts = []
    if label:
        parts.append(label)
    if value is not None and maximum is not None:
        if unit == "byte":
            parts.append(f"{_fmt_bytes(value)}/{_fmt_bytes(maximum)}")
        else:
            parts.append(f"{value}/{maximum}")
            if unit:
                parts.append(unit)
    elif value is not None:
        parts.append(str(value))
        if unit:
            parts.append(unit)
    if rate is not None:
        if unit == "byte":
            parts.append(f"@ {_fmt_bytes(rate)}/s")
        else:
            parts.append(f"@ {rate:.1f}/s")
    return " ".join(parts)


def _fmt_bytes(n) -> str:
    """Format bytes in human-readable binary units."""
    n = float(n)
    for suffix in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024:
            return f"{n:.1f} {suffix}"
        n /= 1024
    return f"{n:.1f} PiB"


def main():
    """CLI entry point for ``nexus-monitor``."""
    parser = argparse.ArgumentParser(
        description="Monitor service – prints broadcast monitoring events from all clients",
    )
    parser.add_argument(
        "--name",
        default=cli_default("name", "monitor", default=f"monitor-{uuid.uuid4().hex[:6]}"),
        help="Client name to register with (default: monitor-<random>)",
    )
    parser.add_argument(
        "--broker-url",
        default=cli_default("broker_url", "monitor", default=None),
        help=f"Broker WebSocket URL (default: {_DEFAULT_URL})",
    )
    parser.add_argument(
        "--reconnect-retries",
        type=int,
        default=cli_default("reconnect_retries", "monitor", default=-1, type_fn=int),
        help="Reconnection attempts on disconnect (-1 = infinite, default: -1)",
    )
    parser.add_argument(
        "--reconnect-delay",
        type=float,
        default=cli_default("reconnect_delay", "monitor", default=2.0, type_fn=float),
        help="Seconds between reconnection attempts (default: 2.0)",
    )
    parser.add_argument(
        "--no-verify",
        action="store_true",
        default=cli_default("no_verify", "monitor", default=False),
        help="Skip TLS certificate verification for wss:// connections",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output raw JSON events instead of formatted text",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=cli_default("debug", "monitor", default=False),
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
            name=args.name,
            url=args.broker_url,
            raw_json=args.json,
            reconnect_retries=args.reconnect_retries,
            reconnect_delay=args.reconnect_delay,
            ssl_verify=not args.no_verify,
        )
    )


async def _run_monitor(name, url, raw_json=False, **client_kwargs):
    """Register as a monitoring service and print broadcast events."""

    console = Console()

    def _on_event(event: dict):
        if raw_json:
            console.print_json(data=event)
            return

        event_type = event.get("type", "info")
        date = event.get("date", "")
        source = event.get("source", "")
        message = event.get("message", "")
        progress = event.get("progress")
        task = event.get("task")

        # Format timestamp (show only time portion if today)
        ts = ""
        if date:
            try:
                dt = datetime.fromisoformat(date)
                ts = dt.strftime("%H:%M:%S")
            except (ValueError, TypeError):
                ts = date

        style = _TYPE_STYLES.get(event_type, "")
        type_tag = f"[{style}]{event_type:<12}[/{style}]" if style else f"{event_type:<12}"
        source_tag = f"[bold magenta]{source}[/bold magenta]" if source else ""

        parts = [f"[dim]{ts}[/dim]", type_tag]
        if source_tag:
            parts.append(source_tag)
        parts.append(message)

        if progress:
            prog_str = _format_progress(progress)
            if prog_str:
                parts.append(f"[dim]({prog_str})[/dim]")

        if task:
            task_name = task.get("name", "")
            if task_name:
                parts.append(f"[dim]task={task_name}[/dim]")

        console.print(" ".join(parts))

    async with Client(name, url, **client_kwargs) as client:
        await client.register_monitor(callback=_on_event)
        console.print(
            f"[bold green]Monitor[/bold green] connected to "
            f"[cyan]{client.url}[/cyan]"
        )
        console.print("[dim]Waiting for events… (Ctrl+C to quit)[/dim]")
        try:
            await client._listener_task
        except asyncio.CancelledError:
            pass
    console.print("[dim]Disconnected.[/dim]")


if __name__ == "__main__":
    main()
