"""CLI tool that registers as a monitoring service and prints broadcast events.

Usage::

    nexus-transfers monitor --broker-url wss://example.com/transfers
    nexus-transfers monitor --filter 'nexus-location-<location_uuid>'   # only events from matching clients
"""

import asyncio
import fnmatch
import uuid
from datetime import datetime

from rich.console import Console

from nexus_transfers._cli import CommandParser
from nexus_transfers._progress import setup_cli_logging
from nexus_transfers.client import _DEFAULT_URL, Client

_TYPE_STYLES = {
    "ok": "bold green",
    "connected": "bold green",
    "disconnected": "bold red",
    "error": "bold red",
    "progress": "bold cyan",
    "warning": "bold yellow",
    "info": "cyan",
}


def _strip_source_prefix(text: str, source: str) -> str:
    """Drop a leading ``"<source>: "`` from *text*.

    Emitters conventionally prefix their own client name to messages and
    progress labels; the monitor prints the event source in its own column,
    so the prefix would appear twice on every line.
    """
    prefix = f"{source}: "
    if source and text.startswith(prefix):
        return text[len(prefix):]
    return text


def _format_progress(progress: dict | None, source: str = "") -> str:
    """Format a progress dict into a compact string."""
    if not progress:
        return ""
    label = _strip_source_prefix(progress.get("label", ""), source)
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
        if unit == "byte":
            parts.append(_fmt_bytes(value))
        else:
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
    """CLI entry point for ``nexus-transfers monitor``."""
    parser = CommandParser(
        "monitor",
        description="Monitor service – prints broadcast monitoring events from all clients",
    )
    parser.option("--name", default=f"monitor-{uuid.uuid4().hex[:6]}",
                  help="Client name to register with (default: monitor-<random>)")
    parser.broker_options(
        f"Broker WebSocket URL (default: {_DEFAULT_URL})",
        no_verify_help="Skip TLS certificate verification for wss:// connections",
    )
    parser.reconnect_options()
    parser.option(
        "--filter", dest="source_filter", key="filter", metavar="PATTERN",
        help="Only show events whose source client name matches this "
             "shell-style wildcard (fnmatch, e.g. 'nexus-location-*'). The "
             "connected-client listing is filtered the same way.",
    )
    parser.add_argument("--json", action="store_true",
                        help="Output raw JSON events instead of formatted text")
    parser.debug_option()
    args = parser.parse_args()

    setup_cli_logging(debug=args.debug)

    asyncio.run(
        _run_monitor(
            name=args.name,
            url=args.broker_url,
            raw_json=args.json,
            source_filter=args.source_filter,
            reconnect_retries=args.reconnect_retries,
            reconnect_delay=args.reconnect_delay,
            ssl_verify=not args.no_verify,
        )
    )


async def _run_monitor(name, url, raw_json=False, source_filter=None,
                       **client_kwargs):
    """Register as a monitoring service and print broadcast events.

    When ``source_filter`` is given, only events whose ``source`` (the client
    name that emitted them) matches the shell-style wildcard are shown. The
    broker stays unaware of what a name stands for — keying client names on
    the unit of work (the Nexus client's ``"nexus-location-<location_uuid>"``)
    lets ``--filter 'nexus-location-<location_uuid>'`` follow a single
    transfer.
    """

    console = Console()

    def _on_event(event: dict):
        if source_filter is not None and not fnmatch.fnmatch(
            event.get("source", ""), source_filter,
        ):
            return

        if raw_json:
            console.print_json(data=event)
            return

        event_type = event.get("type", "info")
        date = event.get("date", "")
        source = event.get("source", "")
        message = _strip_source_prefix(event.get("message", ""), source)
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
            prog_str = _format_progress(progress, source)
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
        clients = await client.list_clients()
        if source_filter is not None:
            clients = [c for c in clients if fnmatch.fnmatch(c, source_filter)]
        if clients:
            console.print(
                f"[bold]Connected clients ({len(clients)}):[/bold] "
                f"[magenta]{', '.join(clients)}[/magenta]"
            )
        else:
            console.print("[dim]No connected clients.[/dim]")
        console.print("[dim]Waiting for events… (Ctrl+C to quit)[/dim]")
        try:
            await client._listener_task
        except asyncio.CancelledError:
            pass
    console.print("[dim]Disconnected.[/dim]")


if __name__ == "__main__":
    main()
