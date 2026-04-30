"""WebSocket RPC client – importable class and interactive CLI."""

import asyncio
import base64
import hashlib
import json
import logging
import os
import traceback
import uuid

_logger = logging.getLogger(__name__)


def _trunc(s, limit=200):
    s = str(s)
    return s[:limit] + "..." if len(s) > limit else s

from pathlib import Path

from dotenv import load_dotenv
from rich.progress import BarColumn, DownloadColumn, Progress, SpinnerColumn, TextColumn, TimeRemainingColumn, TransferSpeedColumn
from websockets.asyncio.client import connect

from nexus_transfers.dispatch import DISPATCH, FileTransfer, make_get_file, make_list_dir

load_dotenv(Path.home() / ".env")

_DEFAULT_URL = os.environ.get("NEXUS_TRANSFERS_URL", "ws://localhost:8766")


class RemoteError(Exception):
    """Raised when a remote function call returns an error."""

    def __init__(self, error, remote_traceback=None):
        super().__init__(error)
        self.remote_traceback = remote_traceback


class Client:
    """Programmatic RPC client that connects to the relay server.

    Parameters
    ----------
    name
        Unique client identifier.
    url
        WebSocket server URL.
    dispatch
        Dispatch table for incoming calls.  Defaults to the built-in
        ``DISPATCH`` table (adder, echo).
    allowed_paths
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
        self._pending = {}
        self._binary_buffers = {}
        self._binary_hashes = {}
        self._binary_checksums = {}
        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            transient=True,
        )
        self._progress_task_ids = {}  # msg_id -> TaskID
        self._listener_task = None

    async def connect(self):
        """Connect to the server and register."""
        extra_headers = {}
        user = os.environ.get("NEXUS_TRANSFERS_USER")
        password = os.environ.get("NEXUS_TRANSFERS_PASSWORD")
        if user and password:
            credentials = base64.b64encode(f"{user}:{password}".encode()).decode()
            extra_headers["Authorization"] = f"Basic {credentials}"
            _logger.debug("Using Basic Auth for user '%s'", user)
        else:
            _logger.debug(
                "No Basic Auth: NEXUS_TRANSFERS_USER=%s, NEXUS_TRANSFERS_PASSWORD=%s",
                "set" if user else "unset",
                "set" if password else "unset",
            )

        _logger.debug("Connecting to %s", self.url)
        self._ws = await connect(self.url, additional_headers=extra_headers).__aenter__()
        reg = json.dumps({"action": "register", "name": self.name})
        _logger.debug("[send] %s", _trunc(reg))
        await self._ws.send(reg)
        resp = json.loads(await self._ws.recv())
        _logger.debug("[recv] %s", _trunc(resp))
        if resp.get("status") != "ok":
            raise RuntimeError(f"Registration failed: {resp.get('error')}")
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

    async def send(self, target_func, *args, **kwargs):
        """Call a function on a remote client and return the result.

        Parameters
        ----------
        target_func
            ``"<target>.<func>"`` string, e.g. ``"a.adder"``.
        *args
            Positional arguments to pass to the remote function.
        **kwargs
            Keyword arguments to pass to the remote function.

        Raises
        ------
        RemoteError
            If the remote function raised an exception.
        """
        if "." not in target_func:
            raise ValueError(f"target_func must be '<target>.<func>', got '{target_func}'")
        target, func_name = target_func.split(".", 1)
        msg_id = str(uuid.uuid4())[:8]
        future = asyncio.get_event_loop().create_future()
        self._pending[msg_id] = future

        payload = {"func": func_name, "args": list(args)}
        if kwargs:
            payload["kwargs"] = kwargs
        envelope = {
            "action": "send",
            "to": target,
            "msg_id": msg_id,
            "payload": payload,
        }
        envelope_str = json.dumps(envelope)
        _logger.debug("[send] %s", _trunc(envelope_str))
        await self._ws.send(envelope_str)
        result = await future
        _logger.debug("[recv] %s", _trunc(result))
        return result

    async def list_clients(self):
        """Return the list of currently connected client names."""
        future = asyncio.get_event_loop().create_future()
        self._pending["__list_clients__"] = future
        msg = json.dumps({"action": "list_clients"})
        _logger.debug("[send] %s", _trunc(msg))
        await self._ws.send(msg)
        return await future

    async def get_directory(self, target, remote_path, local_path, max_concurrent=4):
        """Recursively copy a remote directory to a local path.

        Resumes interrupted transfers by skipping files whose local size
        already matches the remote size.  Files are transferred in parallel.

        Parameters
        ----------
        target
            Name of the remote client, e.g. ``"a"``.
        remote_path
            Path on the remote client to copy from.
        local_path
            Local destination directory.
        max_concurrent
            Maximum number of parallel file transfers.
        """
        # Phase 1: walk the remote tree to collect all file entries
        file_list = []
        await self._walk_remote(target, remote_path, local_path, file_list)

        if not file_list:
            return

        # Phase 2: download files in parallel
        sem = asyncio.Semaphore(max_concurrent)
        copy_task = self._progress.add_task(
            f"[cyan]Copying {remote_path}[/cyan]", total=len(file_list)
        )

        async def _transfer_one(remote_file, local_file):
            async with sem:
                data = await self.send(f"{target}.get_file", remote_file)
                os.makedirs(os.path.dirname(local_file), exist_ok=True)
                with open(local_file, "wb") as f:
                    f.write(data)
                self._progress.advance(copy_task)

        await asyncio.gather(*[
            _transfer_one(rf, lf) for rf, lf in file_list
        ])
        self._progress.remove_task(copy_task)

    async def _walk_remote(self, target, remote_path, local_path, file_list):
        """Recursively list a remote directory and collect files to transfer.

        Parameters
        ----------
        target
            Name of the remote client.
        remote_path
            Current remote directory path.
        local_path
            Corresponding local directory path.
        file_list
            Accumulator list of ``(remote_file, local_file)`` tuples.
        """
        os.makedirs(local_path, exist_ok=True)
        entries = []
        offset = 0
        limit = 1000
        while True:
            _logger.debug("list_dir: %s.list_dir(%r, offset=%d)", target, remote_path, offset)
            page = await self.send(f"{target}.list_dir", remote_path, include_size=False, offset=offset, limit=limit)
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
                # Resume: skip if local file exists with matching size
                if remote_size is not None and os.path.isfile(local_child):
                    local_size = os.path.getsize(local_child)
                    if local_size == remote_size:
                        continue
                file_list.append((remote_child, local_child))

    async def _send_file_chunks(self, target, msg_id, file_transfer):
        """Send file data as binary WebSocket frames.

        Each frame is: ``[2-byte header_len][JSON header][raw chunk data]``.

        Parameters
        ----------
        target
            Name of the recipient client.
        msg_id
            Message ID linking the chunks to the original RPC call.
        file_transfer
            A ``FileTransfer`` instance with path, size, and chunk info.
        """
        fname = os.path.basename(file_transfer.path)
        task_id = self._progress.add_task(f"↑ {fname}", total=file_transfer.size)
        hasher = hashlib.sha256()
        with open(file_transfer.path, "rb") as f:
            for i in range(file_transfer.total_chunks):
                chunk = f.read(file_transfer.chunk_size)
                hasher.update(chunk)
                hdr = {
                    "to": target,
                    "from": self.name,
                    "msg_id": msg_id,
                    "chunk": i,
                    "total_chunks": file_transfer.total_chunks,
                }
                # Include checksum in the last chunk header
                if i == file_transfer.total_chunks - 1:
                    hdr["checksum"] = hasher.hexdigest()
                header = json.dumps(hdr).encode()
                frame = len(header).to_bytes(2, "big") + header + chunk
                await self._ws.send(frame)
                self._progress.advance(task_id, len(chunk))
        self._progress.remove_task(task_id)

    def _receive_binary_chunk(self, raw):
        """Parse a binary frame and buffer the chunk.

        Parameters
        ----------
        raw
            Raw bytes of the binary WebSocket frame.

        Returns
        -------
        tuple or None
            ``(msg_id, assembled_bytes)`` when the transfer is complete,
            otherwise ``None``.
        """
        if len(raw) < 2:
            return None
        header_len = int.from_bytes(raw[:2], "big")
        if len(raw) < 2 + header_len:
            return None
        try:
            header = json.loads(raw[2:2 + header_len])
        except (json.JSONDecodeError, ValueError):
            return None
        chunk_data = raw[2 + header_len:]
        msg_id = header.get("msg_id")
        chunk_idx = header.get("chunk", 0)
        total = header.get("total_chunks", 1)

        if msg_id not in self._binary_buffers:
            self._binary_buffers[msg_id] = [None] * total
            self._binary_hashes[msg_id] = hashlib.sha256()
        self._binary_buffers[msg_id][chunk_idx] = chunk_data
        self._binary_hashes[msg_id].update(chunk_data)

        # Store the sender's checksum from the last chunk
        if "checksum" in header:
            self._binary_checksums[msg_id] = header["checksum"]

        task_id = self._progress_task_ids.get(msg_id)
        if task_id is not None:
            self._progress.advance(task_id, len(chunk_data))

        if all(c is not None for c in self._binary_buffers[msg_id]):
            task_id = self._progress_task_ids.pop(msg_id, None)
            if task_id is not None:
                self._progress.remove_task(task_id)
            data = b"".join(self._binary_buffers.pop(msg_id))
            local_hash = self._binary_hashes.pop(msg_id).hexdigest()
            remote_hash = self._binary_checksums.pop(msg_id, None)
            if remote_hash is not None and local_hash != remote_hash:
                return msg_id, None, f"checksum mismatch: expected {remote_hash}, got {local_hash}"
            return msg_id, data, None
        return None

    async def _listener(self):
        """Background task that handles incoming messages."""
        try:
            async for raw in self._ws:
                # --- Binary frame: buffer chunks ---
                if isinstance(raw, bytes):
                    _logger.debug("[recv] <binary %d bytes>", len(raw))
                    completed = self._receive_binary_chunk(raw)
                    if completed:
                        msg_id, data, error = completed
                        future = self._pending.pop(msg_id, None)
                        if future and not future.done():
                            if error:
                                future.set_exception(RemoteError(error))
                            else:
                                future.set_result(data)
                    continue

                _logger.debug("[recv] %s", _trunc(raw))
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                if not isinstance(msg, dict):
                    continue

                action = msg.get("action")

                # --- Incoming RPC call: dispatch and reply ---
                if action == "message":
                    sender = msg["from"]
                    msg_id = msg.get("msg_id")
                    payload = msg.get("payload", {})
                    func_name = payload.get("func")
                    func_args = payload.get("args", [])
                    func_kwargs = payload.get("kwargs", {})

                    func = self.dispatch.get(func_name)
                    if func is None:
                        result_payload = {
                            "error": f"unknown function '{func_name}'",
                            "traceback": None,
                        }
                        reply = {
                            "action": "reply",
                            "to": sender,
                            "msg_id": msg_id,
                            "payload": result_payload,
                        }
                        _logger.debug("[send] %s", _trunc(json.dumps(reply)))
                        await self._ws.send(json.dumps(reply))
                    else:
                        try:
                            if isinstance(func_args, list):
                                result = func(*func_args, **func_kwargs)
                            else:
                                result = func(func_args, **func_kwargs)
                        except Exception as exc:
                            result_payload = {
                                "error": f"{type(exc).__name__}: {exc}",
                                "traceback": traceback.format_exc(),
                            }
                            reply = {
                                "action": "reply",
                                "to": sender,
                                "msg_id": msg_id,
                                "payload": result_payload,
                            }
                            _logger.debug("[send] %s", _trunc(json.dumps(reply)))
                            await self._ws.send(json.dumps(reply))
                        else:
                            if isinstance(result, FileTransfer):
                                result_payload = {
                                    "result": {
                                        "size": result.size,
                                        "total_chunks": result.total_chunks,
                                        "name": os.path.basename(result.path),
                                    },
                                    "binary_transfer": True,
                                }
                                reply = {
                                    "action": "reply",
                                    "to": sender,
                                    "msg_id": msg_id,
                                    "payload": result_payload,
                                }
                                _logger.debug("[send] %s", _trunc(json.dumps(reply)))
                                await self._ws.send(json.dumps(reply))
                                asyncio.create_task(self._send_file_chunks(sender, msg_id, result))
                            else:
                                result_payload = {"result": result}
                                reply = {
                                    "action": "reply",
                                    "to": sender,
                                    "msg_id": msg_id,
                                    "payload": result_payload,
                                }
                                _logger.debug("[send] %s", _trunc(json.dumps(reply)))
                                await self._ws.send(json.dumps(reply))
                    continue

                # --- Reply to our earlier call ---
                if action == "reply":
                    msg_id = msg.get("msg_id")
                    payload = msg.get("payload", {})

                    # Binary transfer: wait for chunks to arrive
                    if payload.get("binary_transfer"):
                        result_info = payload.get("result", {})
                        total_size = result_info.get("size", 0)
                        total = result_info.get("total_chunks", 0)
                        fname = result_info.get("name", "file")
                        if total > 0:
                            self._progress_task_ids[msg_id] = self._progress.add_task(
                                f"↓ {fname}", total=total_size
                            )
                        if total == 0:
                            future = self._pending.pop(msg_id, None)
                            if future and not future.done():
                                future.set_result(b"")
                        # else: future resolved when last chunk arrives
                        continue

                    future = self._pending.pop(msg_id, None)
                    if future and not future.done():
                        if "error" in payload:
                            future.set_exception(
                                RemoteError(payload["error"], payload.get("traceback"))
                            )
                        else:
                            future.set_result(payload.get("result"))
                    continue

                # --- Server response: list_clients ---
                if action == "list_clients" and msg.get("status") == "ok":
                    future = self._pending.pop("__list_clients__", None)
                    if future and not future.done():
                        future.set_result(msg.get("clients", []))
                    continue

                # --- Server error ---
                if msg.get("status") == "error":
                    msg_id = msg.get("msg_id")
                    future = self._pending.pop(msg_id, None)
                    if future and not future.done():
                        future.set_exception(RemoteError(msg.get("error")))
                    continue

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
    from rich.json import JSON as RichJSON
    from rich.table import Table
    from rich.theme import Theme

    theme = Theme({
        "info": "cyan",
        "success": "green",
        "warning": "yellow",
        "error": "bold red",
        "label": "bold magenta",
        "dim": "dim",
    })
    console = Console(theme=theme)

    async with Client(name, url, allowed_paths=allowed_paths) as client:
        funcs = ", ".join(client.dispatch.keys())
        console.print(f"Connected to [info]{client.url}[/info] as [label]'{name}'[/label]")
        console.print(f"Registered functions: [info]{funcs}[/info]")
        console.print("Type [bold]help[/bold] for available commands.\n")

        # Override the listener to use rich output
        client._listener_task.cancel()
        try:
            await client._listener_task
        except asyncio.CancelledError:
            pass
        asyncio.create_task(_interactive_listener(client, console))

        # --- Cmd shell running in a thread, posting to the event loop ---
        loop = asyncio.get_event_loop()
        stop_event = threading.Event()

        class NexusCmd(cmd_module.Cmd):
            prompt = f"[{name}] > "
            intro = ""

            # -- commands ------------------------------------------------

            def do_send(self, arg):
                """send <target>.<func> [args]  –  Call a remote function."""
                parts = arg.split(None, 1)
                if not parts or "." not in parts[0]:
                    console.print("  Usage: send <target>.<func> [args]", style="warning")
                    console.print("  Example: send b.adder 42", style="dim")
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
                envelope = {
                    "action": "send",
                    "to": target,
                    "msg_id": msg_id,
                    "payload": {"func": func_name, "args": func_args},
                }
                asyncio.run_coroutine_threadsafe(
                    client._ws.send(json.dumps(envelope)), loop
                )

            def do_clients(self, _arg):
                """List connected clients."""
                asyncio.run_coroutine_threadsafe(
                    client._ws.send(json.dumps({"action": "list_clients"})), loop
                )

            def do_mem(self, arg):
                """mem set <key> <val> | mem get <key> | mem dump"""
                parts = arg.split(None, 2)
                cmd = parts[0] if parts else ""
                if cmd == "set" and len(parts) >= 3:
                    msg = {"action": "memory", "cmd": "set", "key": parts[1], "value": parts[2]}
                elif cmd == "get" and len(parts) >= 2:
                    msg = {"action": "memory", "cmd": "get", "key": parts[1]}
                elif cmd == "dump":
                    msg = {"action": "memory", "cmd": "dump"}
                else:
                    console.print("  Usage: mem set <key> <val> | mem get <key> | mem dump", style="warning")
                    return
                asyncio.run_coroutine_threadsafe(
                    client._ws.send(json.dumps(msg)), loop
                )

            def do_quit(self, _arg):
                """Disconnect and exit."""
                stop_event.set()
                return True

            do_EOF = do_quit

            def default(self, line):
                console.print(f"  Unknown command: [warning]{line}[/warning]. Type [bold]help[/bold] for usage.")

            def emptyline(self):
                pass

        def _run_cmd():
            try:
                NexusCmd().cmdloop()
            except KeyboardInterrupt:
                stop_event.set()

        cmd_thread = threading.Thread(target=_run_cmd, daemon=True)
        cmd_thread.start()

        # Wait until the Cmd loop exits
        while not stop_event.is_set():
            await asyncio.sleep(0.1)

    console.print("[dim]Disconnected.[/dim]")


async def _interactive_listener(client, console):
    """Listener for interactive mode that prints results with rich."""
    name = client.name
    try:
        async for raw in client._ws:
            # --- Binary frame ---
            if isinstance(raw, bytes):
                completed = client._receive_binary_chunk(raw)
                if completed:
                    msg_id, data, error = completed
                    if error:
                        console.print(f"\n  [error]\\[binary FAILED][/error] (id={msg_id}) {error}")
                    else:
                        console.print(f"\n  [success]\\[binary complete][/success] (id={msg_id}) {len(data)} bytes")
                    print(f"[{name}] > ", end="", flush=True)
                continue

            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                console.print(f"\n  [dim]<< {raw}[/dim]")
                print(f"[{name}] > ", end="", flush=True)
                continue

            if not isinstance(msg, dict):
                console.print(f"\n  [dim]<< {raw}[/dim]")
                print(f"[{name}] > ", end="", flush=True)
                continue

            action = msg.get("action")
            status = msg.get("status")

            # --- Incoming RPC call: dispatch and reply ---
            if action == "message":
                sender = msg["from"]
                msg_id = msg.get("msg_id")
                payload = msg.get("payload", {})
                func_name = payload.get("func")
                func_args = payload.get("args", [])
                func_kwargs = payload.get("kwargs", {})

                console.print(f"\n  [label]\\[call from {sender}][/label] {func_name}({func_args})")

                func = client.dispatch.get(func_name)
                if func is None:
                    result_payload = {
                        "error": f"unknown function '{func_name}'",
                        "traceback": None,
                    }
                    reply = {
                        "action": "reply",
                        "to": sender,
                        "msg_id": msg_id,
                        "payload": result_payload,
                    }
                    await client._ws.send(json.dumps(reply))
                else:
                    try:
                        if isinstance(func_args, list):
                            result = func(*func_args, **func_kwargs)
                        else:
                            result = func(func_args, **func_kwargs)
                    except Exception as exc:
                        result_payload = {
                            "error": f"{type(exc).__name__}: {exc}",
                            "traceback": traceback.format_exc(),
                        }
                        reply = {
                            "action": "reply",
                            "to": sender,
                            "msg_id": msg_id,
                            "payload": result_payload,
                        }
                        await client._ws.send(json.dumps(reply))
                    else:
                        if isinstance(result, FileTransfer):
                            result_payload = {
                                "result": {
                                    "size": result.size,
                                    "total_chunks": result.total_chunks,
                                    "name": os.path.basename(result.path),
                                },
                                "binary_transfer": True,
                            }
                            reply = {
                                "action": "reply",
                                "to": sender,
                                "msg_id": msg_id,
                                "payload": result_payload,
                            }
                            await client._ws.send(json.dumps(reply))
                            asyncio.create_task(client._send_file_chunks(sender, msg_id, result))
                            console.print(f"  -> [success]sent file:[/success] {result.path} ({result.size} bytes, {result.total_chunks} chunks)")
                        else:
                            result_payload = {"result": result}
                            reply = {
                                "action": "reply",
                                "to": sender,
                                "msg_id": msg_id,
                                "payload": result_payload,
                            }
                            await client._ws.send(json.dumps(reply))
                            console.print(f"  -> [success]replied:[/success] {result_payload}")
                print(f"[{name}] > ", end="", flush=True)
                continue

            # --- Reply to our earlier call ---
            if action == "reply":
                payload = msg.get("payload", {})
                sender = msg.get("from", "?")
                msg_id = msg.get("msg_id", "?")
                if "error" in payload:
                    console.print(f"\n  [error]\\[error from {sender}][/error] (id={msg_id}) {payload['error']}")
                    if payload.get("traceback"):
                        console.print(f"  [dim]{payload['traceback']}[/dim]")
                else:
                    console.print(f"\n  [success]\\[result from {sender}][/success] (id={msg_id}) {json.dumps(payload.get('result'))}")
                print(f"[{name}] > ", end="", flush=True)
                continue

            # --- Server status messages ---
            if status == "ok" and action == "list_clients":
                names = msg.get("clients", [])
                console.print(f"\n  [info]\\[clients][/info] {', '.join(names)}")
                print(f"[{name}] > ", end="", flush=True)
                continue

            if status == "ok" and action == "memory":
                cmd = msg.get("cmd")
                if cmd == "get":
                    console.print(f"\n  [info]\\[memory][/info] {msg['key']} = {msg['value']!r}")
                elif cmd == "dump":
                    console.print(f"\n  [info]\\[memory][/info]")
                    console.print_json(json.dumps(msg["memory"]))
                elif cmd == "set":
                    console.print(f"\n  [info]\\[memory][/info] stored {msg['key']}")
                else:
                    console.print(f"\n  [dim]<< {raw}[/dim]")
                print(f"[{name}] > ", end="", flush=True)
                continue

            if status == "error":
                console.print(f"\n  [error]\\[server error][/error] {msg.get('error')}")
                print(f"[{name}] > ", end="", flush=True)
                continue

            console.print(f"\n  [dim]<< {raw}[/dim]")
            print(f"[{name}] > ", end="", flush=True)

    except Exception:
        pass


def main():
    """CLI entry point for ``transfer-client``."""
    import argparse
    import logging

    parser = argparse.ArgumentParser(description="Transfer RPC client")
    parser.add_argument("--name", required=True, help="Unique client ID")
    parser.add_argument("--server-url", default=None, help=f"Server WebSocket URL (default: {_DEFAULT_URL})")
    parser.add_argument("--allow-path", action="append", default=[], help="Allowed directory for get_file/list_dir (repeatable)")
    parser.add_argument("--interactive", action="store_true", help="Start an interactive prompt (default: headless RPC worker)")
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
