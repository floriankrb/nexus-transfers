"""CLI tool for recursive remote-to-local directory copy.

Usage::

    nexus-transfers copy --from a /remote/dir /local/dir
    nexus-transfers copy --from a src ./mirror --broker-url wss://example.com/transfers
"""

import asyncio
import datetime
import os
import uuid


from nexus_transfers._cli import CommandParser
from nexus_transfers._progress import make_console, setup_cli_logging
from nexus_transfers.client import _DEFAULT_URL, Client


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
    """CLI entry point for ``nexus-transfers copy``."""
    parser = CommandParser(
        "copy",
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
    parser.broker_options(
        f"Broker WebSocket URL (default: {_DEFAULT_URL})",
        name_help="Client name (default: auto-generated)",
        site_help="Site label used in the auto-generated client name instead "
                  "of 'copy'",
        no_verify_help="Skip TLS certificate verification for wss:// "
                       "connections",
    )
    parser.option("--max-concurrent", type=int, default=4,
                  help="Maximum parallel file transfers (default: 4)")
    parser.option("--chunk-size", type=int, default=65536,
                  help="Binary chunk size in bytes for file transfers "
                       "(default: 65536)")
    parser.option("--use-broker", action="store_true",
                  help="Transfer via the WebSocket relay instead of S3 staging")
    parser.reconnect_options()
    parser.peer_options()
    parser.option("--size", action="store_true",
                  help="Show transfer progress in bytes and use size to "
                       "verify resume skips")
    parser.option(
        "--steal", action="store_true",
        help="If a client is already registered under --name, kill it (soft "
             "kill first, then hard kill if it does not exit) and take over "
             "the name. With a name keyed on the transfer (e.g. "
             "nexus-location-<location_uuid>) this guarantees only one copy "
             "of it runs at a time.",
    )
    parser.debug_option()
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
            steal=args.steal,
        )
    )


async def copy(name, broker_url, remote_client, source, target, site=None,
               max_concurrent=4, chunk_size=65536, use_s3=True,
               track_bytes=False, quiet=False, on_monitor=None, steal=False,
               **client_kwargs):
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
    steal:
        If True, displace any peer already registered under ``name`` before
        connecting (soft kill, then hard kill).  Use a name keyed on the unit
        of work (e.g. ``nexus-location-<location_uuid>``) so this acts as a
        per-transfer interlock that
        guarantees only one copy runs at a time.
    **client_kwargs:
        Forwarded to :class:`~nexus_transfers.client.Client`.
    """
    target = os.path.expanduser(target)
    console = make_console()
    if steal:
        from nexus_transfers.claim import claim_name

        await claim_name(
            name, broker_url,
            ssl_verify=client_kwargs.get("ssl_verify", True),
            kill_existing=True,
        )
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
