"""Interactive CLI and headless server entry points."""

import asyncio
import json
import logging
import uuid

from nexus_transfers._progress import setup_cli_logging
from nexus_transfers.config import cli_default
from nexus_transfers.protocol import decode_frame, encode_frame

from ._client import Client, _DEFAULT_URL

# ---------------------------------------------------------------------------
# Headless / Interactive CLI
# ---------------------------------------------------------------------------


async def _serve(name, url, allowed_paths=None, **client_kwargs):
    """Run the client as a headless RPC worker (no interactive prompt)."""
    async with Client(name, url, allowed_paths=allowed_paths, **client_kwargs) as client:
        funcs = ", ".join(client.dispatch.keys())
        print(f"Connected to {url} as '{name}'")
        print(f"Registered functions: {funcs}")
        print("Running in headless mode (Ctrl+C to quit)")
        try:
            await client._listener_task
        except asyncio.CancelledError:
            pass
    print("Disconnected.")


async def _interactive(name, url, allowed_paths=None, **client_kwargs):
    """Run the interactive client loop using cmd.Cmd and rich output."""
    import cmd as cmd_module
    import threading

    from rich.console import Console
    from rich.theme import Theme

    theme = Theme({
        "info": "cyan", "success": "green", "warning": "yellow",
        "error": "bold red", "label": "bold magenta", "dim": "dim",
    })
    console = Console(theme=theme)

    async with Client(name, url, allowed_paths=allowed_paths, **client_kwargs) as client:
        funcs = ", ".join(client.dispatch.keys())
        console.print(f"Connected to [info]{client.url}[/info] as [label]'{name}'[/label]")
        console.print(f"Registered functions: [info]{funcs}[/info]")
        console.print("Type [bold]help[/bold] for available commands.\n")

        client._listener_task.cancel()
        try:
            await client._listener_task
        except asyncio.CancelledError:
            pass
        asyncio.create_task(_interactive_listener(client, console))

        # Stop the progress bar so it doesn't interfere with terminal
        # input in interactive mode.
        client._progress.stop()

        loop = asyncio.get_event_loop()
        stop_event = threading.Event()

        class NexusCmd(cmd_module.Cmd):
            prompt = f"[{name}] > "
            intro = ""

            def do_send(self, arg):
                """send <target>.<func> [args]  –  Call a remote function."""
                parts = arg.split(None, 1)
                if not parts or "." not in parts[0]:
                    console.print("  Usage: send <target>.<func> [args]", style="warning")
                    return
                target, func_name = parts[0].split(".", 1)
                raw_args = parts[1] if len(parts) > 1 else ""
                try:
                    func_args = json.loads(raw_args)
                except (json.JSONDecodeError, ValueError):
                    func_args = raw_args
                if not isinstance(func_args, list):
                    func_args = [func_args] if func_args != "" else []

                msg_id = str(uuid.uuid4())[:8]
                body = {"msg_id": msg_id, "func": func_name, "args": func_args}
                frame = encode_frame(client.name, "call", target, "J",
                                     json.dumps(body).encode())
                asyncio.run_coroutine_threadsafe(client._ws.send(frame), loop)

            def do_clients(self, _arg):
                """List connected clients."""
                frame = encode_frame(client.name, "list_clients", "", "J", b"{}")
                asyncio.run_coroutine_threadsafe(client._ws.send(frame), loop)

            def do_quit(self, _arg):
                """Disconnect and exit."""
                stop_event.set()
                return True

            do_EOF = do_quit

            def default(self, line):
                console.print(
                    f"  Unknown command: [warning]{line}[/warning]. "
                    "Type [bold]help[/bold] for usage."
                )

            def emptyline(self):
                pass

        def _run_cmd():
            try:
                NexusCmd().cmdloop()
            except KeyboardInterrupt:
                stop_event.set()

        cmd_thread = threading.Thread(target=_run_cmd, daemon=True)
        cmd_thread.start()
        while not stop_event.is_set():
            await asyncio.sleep(0.1)

    console.print("[dim]Disconnected.[/dim]")


async def _interactive_listener(client, console):
    """Listener for interactive mode — prints frames with rich formatting."""
    name = client.name
    try:
        async for raw in client._ws:
            if not isinstance(raw, bytes):
                continue
            try:
                _, source, msg_name, _target, encoding, payload = decode_frame(raw)
            except ValueError:
                console.print("\n  [dim]<< <malformed frame>[/dim]")
                print(f"[{name}] > ", end="", flush=True)
                continue

            sender = source or "broker"

            match msg_name:
                case "call":
                    data = json.loads(payload)
                    func_name = data.get("func")
                    func_args = data.get("args", [])
                    console.print(f"\n  [label]\\[call from {sender}][/label] "
                                  f"{func_name}({func_args})")
                    await client._dispatch_call(source, data.get("msg_id"), payload)

                case "reply":
                    data = json.loads(payload)
                    msg_id = data.get("msg_id", "?")
                    if "error" in data:
                        console.print(f"\n  [error]\\[error from {sender}][/error] "
                                      f"(id={msg_id}) {data['error']}")
                        if data.get("traceback"):
                            console.print(f"  [dim]{data['traceback']}[/dim]")
                    elif data.get("s3_transfer"):
                        info = data.get("result", {})
                        console.print(f"\n  [info]\\[s3 staged from {sender}][/info] "
                                      f"(id={msg_id}) key={info.get('s3_key')} "
                                      f"size={info.get('size')}")
                        asyncio.create_task(
                            client._handle_s3_download(msg_id, source, info)
                        )
                    else:
                        console.print(f"\n  [success]\\[result from {sender}][/success] "
                                      f"(id={msg_id}) {json.dumps(data.get('result'))}")

                case "chunk":
                    completed = client._receive_chunk_payload(payload)
                    if completed:
                        msg_id, data, error = completed
                        if error:
                            console.print(f"\n  [error]\\[binary FAILED][/error] "
                                          f"(id={msg_id}) {error}")
                        else:
                            console.print(f"\n  [success]\\[binary complete][/success] "
                                          f"(id={msg_id}) {len(data)} bytes")

                case "list_clients":
                    names = json.loads(payload).get("clients", [])
                    console.print(f"\n  [info]\\[clients][/info] {', '.join(names)}")

                case "error":
                    data = json.loads(payload)
                    console.print(f"\n  [error]\\[broker error][/error] {data.get('error')}")

                case _:
                    console.print(f"\n  [dim]<< msg={msg_name!r} from {sender}[/dim]")

            print(f"[{name}] > ", end="", flush=True)

    except Exception:
        pass


def main():
    """CLI entry point for ``nexus-client``."""
    import argparse

    parser = argparse.ArgumentParser(description="Transfer RPC client")
    parser.add_argument("--name", required=True, help="Unique client ID")
    parser.add_argument("--broker-url",
                        default=cli_default("broker_url", "client", default=None),
                        help=f"Broker WebSocket URL (default: {_DEFAULT_URL})")
    parser.add_argument("--allow-path", action="append", default=[],
                        help="Allowed directory for get_file/list_dir (repeatable)")
    parser.add_argument("--interactive", action="store_true",
                        default=cli_default("interactive", "client", default=False),
                        help="Start an interactive prompt (default: headless RPC worker)")
    parser.add_argument("--reconnect-retries", type=int,
                        default=cli_default("reconnect_retries", "client", default=-1, type_fn=int),
                        help="Reconnection attempts on disconnect (-1 = infinite, default: -1)")
    parser.add_argument("--reconnect-delay", type=float,
                        default=cli_default("reconnect_delay", "client", default=2.0, type_fn=float),
                        help="Seconds between reconnection attempts (default: 2.0)")
    parser.add_argument("--peer-retries", type=int,
                        default=cli_default("peer_retries", "client", default=-1, type_fn=int),
                        help="Retries when target peer is not found (-1 = infinite, default: -1)")
    parser.add_argument("--peer-delay", type=float,
                        default=cli_default("peer_delay", "client", default=2.0, type_fn=float),
                        help="Seconds between peer-not-found retries (default: 2.0)")
    parser.add_argument("--call-timeout", type=float,
                        default=cli_default("call_timeout", "client", default=None, type_fn=float),
                        help="Timeout in seconds for RPC calls (default: no timeout)")
    parser.add_argument("--no-verify", action="store_true",
                        default=cli_default("no_verify", "client", default=False),
                        help="Skip TLS certificate verification for wss:// connections")
    parser.add_argument("--debug", action="store_true",
                        default=cli_default("debug", "client", default=False),
                        help="Enable debug logging")
    args = parser.parse_args()
    setup_cli_logging(debug=args.debug)
    client_kwargs = dict(
        reconnect_retries=args.reconnect_retries,
        reconnect_delay=args.reconnect_delay,
        peer_retries=args.peer_retries,
        peer_delay=args.peer_delay,
        call_timeout=args.call_timeout,
        ssl_verify=not args.no_verify,
    )
    coro = (
        _interactive(args.name, args.broker_url,
                     allowed_paths=args.allow_path or None, **client_kwargs)
        if args.interactive
        else _serve(args.name, args.broker_url,
                    allowed_paths=args.allow_path or None, **client_kwargs)
    )
    asyncio.run(coro)


if __name__ == "__main__":
    main()
