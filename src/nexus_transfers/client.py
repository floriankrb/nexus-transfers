"""WebSocket RPC client – importable class and interactive CLI."""

import asyncio
import base64
import hashlib
import json
import logging
import os
import traceback
import uuid
from pathlib import Path

from dotenv import load_dotenv
from rich.progress import (BarColumn, DownloadColumn, Progress, ProgressColumn,
                           SpinnerColumn, TextColumn, TimeRemainingColumn)
from rich.text import Text
from websockets.asyncio.client import connect

from nexus_transfers.dispatch import DISPATCH, FileTransfer, make_get_file, make_list_dir
from nexus_transfers.protocol import decode_frame, encode_frame

load_dotenv(Path.home() / ".env")

_logger = logging.getLogger(__name__)
_DEFAULT_URL = os.environ.get("NEXUS_TRANSFERS_URL", "ws://localhost:8766")


def _trunc(s, limit=200):
    s = str(s)
    return s[:limit] + "..." if len(s) > limit else s


def _write_file(path, data):
    with open(path, "wb") as fh:
        fh.write(data)


def _fmt_binary(n: float) -> str:
    """Format a byte count using binary prefixes (KiB, MiB, GiB, TiB, PiB)."""
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if n < 1024:
            return f"{n:.2f} {unit}"
        n /= 1024
    return f"{n:.2f} PiB"


class _BinarySpeedColumn(ProgressColumn):
    """Renders transfer speed using binary prefixes (KiB/s, MiB/s, …)."""

    def render(self, task) -> Text:
        speed = task.finished_speed or task.speed
        if speed is None:
            return Text("? /s", style="progress.data.speed")
        return Text(_fmt_binary(speed) + "/s", style="progress.data.speed")


class RemoteError(Exception):
    """Raised when a remote function call returns an error."""

    def __init__(self, error, remote_traceback=None):
        super().__init__(error)
        self.remote_traceback = remote_traceback


class Client:
    """Programmatic RPC client that connects to the relay server.

    Parameters
    ----------
    name:
        Unique client identifier.
    url:
        WebSocket server URL.
    dispatch:
        Dispatch table for incoming calls.  Defaults to the built-in
        ``DISPATCH`` table (adder, echo).
    allowed_paths:
        List of directories that ``get_file`` and ``list_dir`` may access.
        If ``None``, file operations are disabled.
    """

    def __init__(self, name, url=None, dispatch=None, allowed_paths=None):
        self.name = name
        self.url = url or _DEFAULT_URL
        self.dispatch = dispatch if dispatch is not None else dict(DISPATCH)
        self.allowed_paths = [os.path.realpath(p) for p in allowed_paths] if allowed_paths else []
        if self.allowed_paths:
            self.dispatch["get_file"] = make_get_file(self.allowed_paths)
            self.dispatch["list_dir"] = make_list_dir(self.allowed_paths)
        self._ws = None
        self._pending: dict[str, asyncio.Future] = {}
        self._binary_buffers: dict = {}
        self._binary_received: dict = {}
        self._binary_hashes: dict = {}
        self._binary_checksums: dict = {}
        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            DownloadColumn(binary_units=True),
            _BinarySpeedColumn(),
            TimeRemainingColumn(),
            transient=True,
        )
        self._progress_task_ids: dict = {}
        self._listener_task = None

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connect(self):
        """Connect to the server and register."""
        extra_headers = {}
        user = os.environ.get("NEXUS_TRANSFERS_USER")
        password = os.environ.get("NEXUS_TRANSFERS_PASSWORD")
        if user and password:
            credentials = base64.b64encode(f"{user}:{password}".encode()).decode()
            extra_headers["Authorization"] = f"Basic {credentials}"

        _logger.debug("Connecting to %s as '%s'", self.url, self.name)
        self._ws = await connect(self.url, additional_headers=extra_headers).__aenter__()

        reg = encode_frame(self.name, "register", "", "J", b"{}")
        await self._ws.send(reg)
        ack = await self._ws.recv()

        _, _src, msg_name, _tgt, _enc, payload = decode_frame(ack)
        match msg_name:
            case "register":
                pass
            case "error":
                raise RuntimeError(f"Registration failed: {json.loads(payload).get('error')}")
            case _:
                raise RuntimeError(f"Unexpected registration response: {msg_name!r}")

        self._progress.start()
        self._listener_task = asyncio.create_task(self._listener())

    async def close(self):
        """Disconnect from the server."""
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        self._progress.stop()
        if self._ws:
            await self._ws.close()

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *exc):
        await self.close()

    # ------------------------------------------------------------------
    # Outgoing calls
    # ------------------------------------------------------------------

    async def send(self, target_func, *args, **kwargs):
        """Call a function on a remote client and return the result.

        Parameters
        ----------
        target_func:
            ``"<target>.<func>"`` string, e.g. ``"a.adder"``.
        *args:
            Positional arguments forwarded to the remote function.
        **kwargs:
            Keyword arguments forwarded to the remote function.

        Raises
        ------
        RemoteError
            If the remote function raised an exception.
        """
        if "." not in target_func:
            raise ValueError(f"target_func must be '<target>.<func>', got '{target_func}'")
        target, func_name = target_func.split(".", 1)
        msg_id = str(uuid.uuid4())[:8]
        future = asyncio.get_running_loop().create_future()
        self._pending[msg_id] = future

        body = {"msg_id": msg_id, "func": func_name, "args": list(args)}
        if kwargs:
            body["kwargs"] = kwargs
        frame = encode_frame(self.name, "call", target, "J", json.dumps(body).encode())
        _logger.debug("[send] call %s -> %s.%s (id=%s)", self.name, target, func_name, msg_id)
        await self._ws.send(frame)
        return await future

    async def list_clients(self):
        """Return the list of currently connected client names."""
        future = asyncio.get_running_loop().create_future()
        self._pending["__list_clients__"] = future
        frame = encode_frame(self.name, "list_clients", "", "J", b"{}")
        await self._ws.send(frame)
        return await future

    # ------------------------------------------------------------------
    # Directory / file transfer helpers
    # ------------------------------------------------------------------

    async def get_directory(self, target, remote_path, local_path,
                            max_concurrent=4, chunk_size=65536):
        """Recursively copy a remote directory to a local path.

        Resumes interrupted transfers by skipping files whose local size
        already matches the remote size.

        Parameters
        ----------
        target:
            Name of the remote client.
        remote_path:
            Path on the remote client to copy from.
        local_path:
            Local destination directory.
        max_concurrent:
            Maximum number of parallel file transfers.
        chunk_size:
            Binary chunk size in bytes sent to the remote ``get_file``.
        """
        file_list = []
        await self._walk_remote(target, remote_path, local_path, file_list)
        if not file_list:
            return

        sem = asyncio.Semaphore(max_concurrent)
        copy_task = self._progress.add_task(
            f"[cyan]Copying {remote_path}[/cyan]", total=len(file_list)
        )
        loop = asyncio.get_running_loop()
        total_bytes = 0
        start = loop.time()

        async def _transfer_one(remote_file, local_file):
            nonlocal total_bytes
            async with sem:
                data = await self.send(f"{target}.get_file", remote_file,
                                       chunk_size=chunk_size)
                os.makedirs(os.path.dirname(local_file), exist_ok=True)
                await loop.run_in_executor(None, _write_file, local_file, data)
                total_bytes += len(data)
                self._progress.advance(copy_task)

        await asyncio.gather(*[_transfer_one(rf, lf) for rf, lf in file_list])
        self._progress.remove_task(copy_task)

        elapsed = loop.time() - start
        rate = total_bytes / elapsed if elapsed > 0 else 0
        self._progress.console.print(
            f"Transferred [bold]{_fmt_binary(total_bytes)}[/bold] "
            f"in [bold]{elapsed:.1f}s[/bold] "
            f"([bold]{_fmt_binary(rate)}/s[/bold])"
        )

    async def _walk_remote(self, target, remote_path, local_path, file_list):
        os.makedirs(local_path, exist_ok=True)
        entries = []
        offset = 0
        limit = 10000
        while True:
            page = await self.send(f"{target}.list_dir", remote_path,
                                   include_size=False, offset=offset, limit=limit)
            entries.extend(page)
            if len(page) < limit:
                break
            offset += len(page)
        for entry in entries:
            name = entry["name"]
            remote_child = f"{remote_path}/{name}" if remote_path != "." else name
            local_child = os.path.join(local_path, name)
            if entry["type"] == "dir":
                await self._walk_remote(target, remote_child, local_child, file_list)
            else:
                remote_size = entry.get("size")
                if remote_size is not None and os.path.isfile(local_child):
                    if os.path.getsize(local_child) == remote_size:
                        continue
                file_list.append((remote_child, local_child))

    # ------------------------------------------------------------------
    # Binary chunk transfer (sending side)
    # ------------------------------------------------------------------

    async def _send_file_chunks(self, target: str, msg_id: str, file_transfer: FileTransfer):
        """Send file data as a sequence of R-encoded binary frames.

        Chunk payload layout (inside the R-encoded frame):
          [2 bytes: json_header_len][json_header][raw chunk bytes]

        The JSON header carries msg_id, chunk index, total_chunks, and
        (on the final chunk) a SHA-256 checksum.
        """
        fname = os.path.basename(file_transfer.path)
        task_id = self._progress.add_task(f"↑ {fname}", total=file_transfer.size)
        loop = asyncio.get_running_loop()
        hasher = hashlib.sha256()
        with open(file_transfer.path, "rb") as f:
            for i in range(file_transfer.total_chunks):
                chunk = await loop.run_in_executor(None, f.read, file_transfer.chunk_size)
                hasher.update(chunk)
                hdr: dict = {"msg_id": msg_id, "chunk": i,
                              "total_chunks": file_transfer.total_chunks}
                if i == file_transfer.total_chunks - 1:
                    hdr["checksum"] = hasher.hexdigest()
                hdr_bytes = json.dumps(hdr).encode()
                raw_payload = len(hdr_bytes).to_bytes(2, "big") + hdr_bytes + chunk
                frame = encode_frame(self.name, "chunk", target, "R", raw_payload)
                await self._ws.send(frame)
                self._progress.advance(task_id, len(chunk))
                await asyncio.sleep(0)
        self._progress.remove_task(task_id)

    # ------------------------------------------------------------------
    # Binary chunk transfer (receiving side)
    # ------------------------------------------------------------------

    def _receive_chunk_payload(self, raw_payload: bytes):
        """Parse a chunk payload and buffer the data.

        Returns
        -------
        tuple or None
            ``(msg_id, assembled_bytes, error_or_None)`` when all chunks
            have arrived, otherwise ``None``.
        """
        if len(raw_payload) < 2:
            return None
        hdr_len = int.from_bytes(raw_payload[:2], "big")
        if len(raw_payload) < 2 + hdr_len:
            return None
        try:
            hdr = json.loads(raw_payload[2:2 + hdr_len])
        except (json.JSONDecodeError, ValueError):
            return None
        chunk_data = raw_payload[2 + hdr_len:]
        msg_id = hdr.get("msg_id")
        chunk_idx = hdr.get("chunk", 0)
        total = hdr.get("total_chunks", 1)

        if msg_id not in self._binary_buffers:
            self._binary_buffers[msg_id] = [None] * total
            self._binary_received[msg_id] = 0
            self._binary_hashes[msg_id] = hashlib.sha256()
        self._binary_buffers[msg_id][chunk_idx] = chunk_data
        self._binary_received[msg_id] += 1
        self._binary_hashes[msg_id].update(chunk_data)

        if "checksum" in hdr:
            self._binary_checksums[msg_id] = hdr["checksum"]

        task_id = self._progress_task_ids.get(msg_id)
        if task_id is not None:
            self._progress.advance(task_id, len(chunk_data))

        if self._binary_received[msg_id] == total:
            task_id = self._progress_task_ids.pop(msg_id, None)
            if task_id is not None:
                self._progress.remove_task(task_id)
            self._binary_received.pop(msg_id)
            data = b"".join(self._binary_buffers.pop(msg_id))
            local_hash = self._binary_hashes.pop(msg_id).hexdigest()
            remote_hash = self._binary_checksums.pop(msg_id, None)
            if remote_hash is not None and local_hash != remote_hash:
                return msg_id, None, f"checksum mismatch: expected {remote_hash}, got {local_hash}"
            return msg_id, data, None
        return None

    # ------------------------------------------------------------------
    # Shared RPC dispatch (used by both listener modes)
    # ------------------------------------------------------------------

    async def _dispatch_call(self, sender: str, msg_id: str, payload: bytes):
        """Dispatch an incoming RPC call and send the reply frame."""
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            body = {"msg_id": msg_id, "error": "invalid JSON payload", "traceback": None}
            await self._ws.send(encode_frame(self.name, "reply", sender, "J",
                                             json.dumps(body).encode()))
            return

        func_name = data.get("func")
        func_args = data.get("args", [])
        func_kwargs = data.get("kwargs", {})

        func = self.dispatch.get(func_name)
        if func is None:
            body = {"msg_id": msg_id, "error": f"unknown function '{func_name}'",
                    "traceback": None}
            await self._ws.send(encode_frame(self.name, "reply", sender, "J",
                                             json.dumps(body).encode()))
            return

        try:
            result = func(*func_args, **func_kwargs) if isinstance(func_args, list) \
                else func(func_args, **func_kwargs)
        except Exception as exc:
            body = {"msg_id": msg_id, "error": f"{type(exc).__name__}: {exc}",
                    "traceback": traceback.format_exc()}
            await self._ws.send(encode_frame(self.name, "reply", sender, "J",
                                             json.dumps(body).encode()))
            return

        if isinstance(result, FileTransfer):
            body = {
                "msg_id": msg_id,
                "result": {
                    "size": result.size,
                    "total_chunks": result.total_chunks,
                    "name": os.path.basename(result.path),
                },
                "binary_transfer": True,
            }
            await self._ws.send(encode_frame(self.name, "reply", sender, "J",
                                             json.dumps(body).encode()))
            asyncio.create_task(self._send_file_chunks(sender, msg_id, result))
        else:
            body = {"msg_id": msg_id, "result": result}
            await self._ws.send(encode_frame(self.name, "reply", sender, "J",
                                             json.dumps(body).encode()))

    # ------------------------------------------------------------------
    # Background listener (headless / programmatic mode)
    # ------------------------------------------------------------------

    async def _listener(self):
        """Background task that handles incoming frames."""
        try:
            async for raw in self._ws:
                if not isinstance(raw, bytes):
                    continue
                try:
                    _, source, msg_name, _target, encoding, payload = decode_frame(raw)
                except ValueError:
                    continue

                _logger.debug("[recv] %s from %s", msg_name, source or "server")

                match msg_name:
                    case "call":
                        data = json.loads(payload)
                        await self._dispatch_call(source, data.get("msg_id"), payload)

                    case "reply":
                        data = json.loads(payload)
                        msg_id = data.get("msg_id")
                        if data.get("binary_transfer"):
                            result_info = data.get("result", {})
                            total_size = result_info.get("size", 0)
                            total = result_info.get("total_chunks", 0)
                            fname = result_info.get("name", "file")
                            if total > 0:
                                self._progress_task_ids[msg_id] = self._progress.add_task(
                                    f"↓ {fname}", total=total_size
                                )
                            else:
                                future = self._pending.pop(msg_id, None)
                                if future and not future.done():
                                    future.set_result(b"")
                        else:
                            future = self._pending.pop(msg_id, None)
                            if future and not future.done():
                                if "error" in data:
                                    future.set_exception(
                                        RemoteError(data["error"], data.get("traceback"))
                                    )
                                else:
                                    future.set_result(data.get("result"))

                    case "chunk":
                        completed = self._receive_chunk_payload(payload)
                        if completed:
                            msg_id, data, error = completed
                            future = self._pending.pop(msg_id, None)
                            if future and not future.done():
                                if error:
                                    future.set_exception(RemoteError(error))
                                else:
                                    future.set_result(data)

                    case "list_clients":
                        data = json.loads(payload)
                        future = self._pending.pop("__list_clients__", None)
                        if future and not future.done():
                            future.set_result(data.get("clients", []))

                    case "error":
                        data = json.loads(payload)
                        error = data.get("error", "unknown error")
                        msg_id = data.get("msg_id")
                        future = self._pending.pop(msg_id, None)
                        if future and not future.done():
                            future.set_exception(RemoteError(error))

                    case _:
                        _logger.debug("[recv] unhandled msg_name=%r from %s", msg_name, source)

        except asyncio.CancelledError:
            pass
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Headless / Interactive CLI
# ---------------------------------------------------------------------------

async def _serve(name, url, allowed_paths=None):
    """Run the client as a headless RPC worker (no interactive prompt)."""
    async with Client(name, url, allowed_paths=allowed_paths) as client:
        funcs = ", ".join(client.dispatch.keys())
        print(f"Connected to {url} as '{name}'")
        print(f"Registered functions: {funcs}")
        print("Running in headless mode (Ctrl+C to quit)")
        try:
            await client._listener_task
        except asyncio.CancelledError:
            pass
    print("Disconnected.")


async def _interactive(name, url, allowed_paths=None):
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

    async with Client(name, url, allowed_paths=allowed_paths) as client:
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
                console.print(f"\n  [dim]<< <malformed frame>[/dim]")
                print(f"[{name}] > ", end="", flush=True)
                continue

            sender = source or "server"

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
                    console.print(f"\n  [error]\\[server error][/error] {data.get('error')}")

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
    parser.add_argument("--server-url", default=None,
                        help=f"Server WebSocket URL (default: {_DEFAULT_URL})")
    parser.add_argument("--allow-path", action="append", default=[],
                        help="Allowed directory for get_file/list_dir (repeatable)")
    parser.add_argument("--interactive", action="store_true",
                        help="Start an interactive prompt (default: headless RPC worker)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    from rich.logging import RichHandler
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        handlers=[RichHandler(rich_tracebacks=True)],
    )
    coro = (
        _interactive(args.name, args.server_url, allowed_paths=args.allow_path or None)
        if args.interactive
        else _serve(args.name, args.server_url, allowed_paths=args.allow_path or None)
    )
    asyncio.run(coro)


if __name__ == "__main__":
    main()
