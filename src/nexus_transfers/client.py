"""WebSocket RPC client – importable class and interactive CLI."""

import asyncio
import base64
import hashlib
import json
import logging
import os
import shutil
import ssl
import tempfile
import traceback
import uuid
from pathlib import Path

from dotenv import load_dotenv
from rich.progress import (BarColumn, Progress, ProgressColumn,
                           SpinnerColumn, TextColumn, TimeRemainingColumn)
from rich.text import Text
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosedError

from nexus_transfers.dispatch import (DISPATCH, FileTransfer, S3Transfer,
                                       make_get_file, make_list_dir)
from nexus_transfers.protocol import decode_frame, encode_frame

load_dotenv(Path.home() / ".env")

_logger = logging.getLogger(__name__)
_DEFAULT_URL = os.environ.get("NEXUS_TRANSFERS_URL", "ws://localhost:8766")


def _trunc(s, limit=200):
    s = str(s)
    return s[:limit] + "..." if len(s) > limit else s


def _write_file(path, data):
    """Write *data* to *path* atomically via a temp file + rename."""
    dirpath = os.path.dirname(path)
    fd, tmp = tempfile.mkstemp(dir=dirpath)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
        os.replace(tmp, path)
    except BaseException:
        os.unlink(tmp)
        raise


def _fmt_binary(n: float) -> str:
    """Format a byte count using binary prefixes (KiB, MiB, GiB, TiB, PiB)."""
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if n < 1024:
            return f"{n:.2f} {unit}"
        n /= 1024
    return f"{n:.2f} PiB"


class _CountOrBytesColumn(ProgressColumn):
    """Shows 'X / N files' when task has unit='files', otherwise 'X.x MiB / Y.y MiB'.

    When the total is unknown (still being discovered), shows just 'X files'.
    """

    def render(self, task) -> Text:
        completed = int(task.completed)
        if task.fields.get("unit") == "files":
            if task.total is None:
                return Text(f"{completed} files")
            return Text(f"{completed} / {int(task.total)} files")
        total = int(task.total) if task.total is not None else 0
        return Text(f"{_fmt_binary(completed)} / {_fmt_binary(total)}")


class _BinarySpeedColumn(ProgressColumn):
    """Renders transfer speed; files/s for unit='files' tasks, binary bytes/s otherwise."""

    def render(self, task) -> Text:
        speed = task.finished_speed or task.speed
        if speed is None:
            return Text("? /s", style="progress.data.speed")
        if task.fields.get("unit") == "files":
            return Text(f"{speed:.1f} files/s", style="progress.data.speed")
        return Text(_fmt_binary(speed) + "/s", style="progress.data.speed")


class RemoteError(Exception):
    """Raised when a remote function call returns an error."""

    def __init__(self, error, remote_traceback=None):
        super().__init__(error)
        self.remote_traceback = remote_traceback


class PeerNotFoundError(RemoteError):
    """Raised when the target peer is not registered on the relay."""


class NameTakenError(Exception):
    """Raised when another client already holds this name on the relay."""


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
    reconnect_retries:
        Number of reconnection attempts when the server connection drops.
        ``-1`` means infinite retries.  ``0`` disables reconnection.
    reconnect_delay:
        Seconds to wait between reconnection attempts.
    peer_retries:
        Number of retries when a target peer is not yet registered.
        ``-1`` means infinite retries.  ``0`` means no retries.
    peer_delay:
        Seconds to wait between peer-not-found retries.
    call_timeout:
        Timeout in seconds for a single RPC call (waiting for a reply).
        ``None`` means no timeout.
    ssl_verify:
        If False, skip TLS certificate verification for ``wss://``
        connections.  Defaults to True.
    """

    def __init__(self, name, url=None, dispatch=None, allowed_paths=None,
                 reconnect_retries=0, reconnect_delay=2.0,
                 peer_retries=0, peer_delay=2.0,
                 call_timeout=None, ssl_verify=True):
        self.name = name
        self.url = url or _DEFAULT_URL
        self.dispatch = dispatch if dispatch is not None else dict(DISPATCH)
        self.allowed_paths = [os.path.realpath(p) for p in allowed_paths] if allowed_paths else []
        self.reconnect_retries = reconnect_retries
        self.reconnect_delay = reconnect_delay
        self.peer_retries = peer_retries
        self.peer_delay = peer_delay
        self.call_timeout = call_timeout
        self.ssl_verify = ssl_verify
        self._s3_keys: set[str] = set()
        if self.allowed_paths:
            self.dispatch["get_file"] = make_get_file(self.allowed_paths)
            self.dispatch["list_dir"] = make_list_dir(self.allowed_paths)
            self.dispatch["s3_cleanup"] = self._s3_cleanup
        self._ws = None
        self._pending: dict[str, asyncio.Future] = {}
        self._binary_buffers: dict = {}
        self._binary_received: dict = {}
        self._binary_hashes: dict = {}
        self._binary_checksums: dict = {}
        self._reconnect_lock = asyncio.Lock()
        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            _CountOrBytesColumn(),
            _BinarySpeedColumn(),
            TimeRemainingColumn(),
            transient=True,
        )
        self._progress_task_ids: dict = {}
        self._listener_task = None
        self._closed = False

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def _register(self):
        """Send a register frame and wait for the acknowledgement."""
        extra_headers = {}
        user = os.environ.get("NEXUS_TRANSFERS_USER")
        password = os.environ.get("NEXUS_TRANSFERS_PASSWORD")
        if user and password:
            credentials = base64.b64encode(f"{user}:{password}".encode()).decode()
            extra_headers["Authorization"] = f"Basic {credentials}"

        _logger.debug("Connecting to %s as '%s'", self.url, self.name)
        connect_kwargs: dict = {"additional_headers": extra_headers,
                                "max_size": 104_857_600}  # 100 MiB
        if not self.ssl_verify and self.url.startswith("wss://"):
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            connect_kwargs["ssl"] = ssl_context
        self._ws = await connect(self.url, **connect_kwargs).__aenter__()

        reg = encode_frame(self.name, "register", "", "J", b"{}")
        await self._ws.send(reg)
        ack = await asyncio.wait_for(self._ws.recv(), timeout=30)

        _, _src, msg_name, _tgt, _enc, payload = decode_frame(ack)
        match msg_name:
            case "register":
                pass
            case "error":
                error_msg = json.loads(payload).get("error", "")
                if "already taken" in error_msg:
                    raise NameTakenError(error_msg)
                raise RuntimeError(f"Registration failed: {error_msg}")
            case _:
                raise RuntimeError(f"Unexpected registration response: {msg_name!r}")

    async def connect(self):
        """Connect to the server and register, with optional retries."""
        await self._register()
        self._progress.start()
        self._listener_task = asyncio.create_task(self._listener())

    async def close(self):
        """Disconnect from the server."""
        self._closed = True
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
    # Reconnection
    # ------------------------------------------------------------------

    async def _reconnect(self):
        """Attempt to re-establish the server connection.

        Retries up to ``reconnect_retries`` times (``-1`` = infinite).
        Raises ``NameTakenError`` immediately if the name is in use.
        """
        attempt = 0
        while True:
            attempt += 1
            if self.reconnect_retries != -1 and attempt > self.reconnect_retries:
                _logger.error("Reconnection failed after %d attempts", attempt - 1)
                raise ConnectionError("reconnection retries exhausted")
            _logger.info("Reconnecting (attempt %d) in %.1fs …",
                         attempt, self.reconnect_delay)
            await asyncio.sleep(self.reconnect_delay)
            try:
                await self._register()
                _logger.info("Reconnected successfully")
                return
            except NameTakenError:
                raise
            except Exception as exc:
                _logger.warning("Reconnection attempt %d failed: %s", attempt, exc)

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
        PeerNotFoundError
            If the target peer is not registered and retries are exhausted.
        asyncio.TimeoutError
            If ``call_timeout`` is set and the reply does not arrive in time.
        """
        if "." not in target_func:
            raise ValueError(f"target_func must be '<target>.<func>', got '{target_func}'")

        attempt = 0
        while True:
            target, func_name = target_func.split(".", 1)
            msg_id = str(uuid.uuid4())[:8]
            future = asyncio.get_running_loop().create_future()
            self._pending[msg_id] = future

            body = {"msg_id": msg_id, "func": func_name, "args": list(args)}
            if kwargs:
                body["kwargs"] = kwargs
            frame = encode_frame(self.name, "call", target, "J", json.dumps(body).encode())
            _logger.debug("[send] call %s -> %s.%s (id=%s)", self.name, target, func_name, msg_id)

            try:
                await asyncio.wait_for(self._ws.send(frame), timeout=30)
            except Exception as exc:
                self._pending.pop(msg_id, None)
                raise ConnectionError(f"failed to send: {exc}") from exc

            try:
                if self.call_timeout is not None:
                    result = await asyncio.wait_for(future, self.call_timeout)
                else:
                    result = await future
                return result
            except asyncio.TimeoutError:
                self._pending.pop(msg_id, None)
                raise
            except PeerNotFoundError:
                self._pending.pop(msg_id, None)
                attempt += 1
                if self.peer_retries != -1 and attempt > self.peer_retries:
                    raise
                _logger.info("Peer '%s' not found, retrying in %.1fs (attempt %d) …",
                             target, self.peer_delay, attempt)
                await asyncio.sleep(self.peer_delay)

    async def list_clients(self):
        """Return the list of currently connected client names."""
        future = asyncio.get_running_loop().create_future()
        self._pending["__list_clients__"] = future
        frame = encode_frame(self.name, "list_clients", "", "J", b"{}")
        await self._ws.send(frame)
        return await future

    _MONITOR_TIMEOUT = 0.5

    async def monitor(self, message: str, status: str | None = None):
        """Send a monitoring message to the ``monitor`` peer.

        This is fire-and-forget: if no monitor peer is connected, or the
        call times out, the error is silently ignored so it never blocks
        the caller.

        Parameters
        ----------
        message
            Free-form message string.
        status
            Optional status label (e.g. ``"ok"``, ``"error"``, ``"progress"``).
        """
        try:
            kwargs = {}
            if status is not None:
                kwargs["status"] = status
            await asyncio.wait_for(
                self.send("monitor.log", message, **kwargs),
                timeout=self._MONITOR_TIMEOUT,
            )
        except Exception:
            _logger.debug("Monitor send failed", exc_info=True)

    # ------------------------------------------------------------------
    # Directory / file transfer helpers
    # ------------------------------------------------------------------

    async def get_directory(self, target, remote_path, local_path,
                            max_concurrent=4, chunk_size=65536,
                            use_s3=True, s3_prefix=None, track_bytes=False):
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
            Binary chunk size in bytes sent to the remote ``get_file``
            (only used when ``use_s3`` is False).
        use_s3:
            If True (default), stage transfers through S3.  Set to False
            to send file bytes over the WebSocket relay instead.
        s3_prefix:
            Optional prefix prepended to S3 keys for this transfer batch.
        track_bytes:
            If True, show progress in bytes instead of file count.  This
            requires fetching remote file sizes during the directory walk.
        """
        label = os.path.basename(remote_path.rstrip("/")) or remote_path

        walk_task = self._progress.add_task(
            f"[magenta]Listing {label}[/magenta]", total=None, unit="files"
        )

        # Queue feeds files from _walk_remote to transfer workers as
        # pages are discovered, so downloads start while listing is
        # still in progress.
        queue: asyncio.Queue = asyncio.Queue()
        discovered = 0
        skipped = 0
        skipped_bytes = 0

        async def _walk_and_enqueue():
            nonlocal discovered, skipped, skipped_bytes
            await self._walk_remote_streamed(
                target, remote_path, local_path, queue,
                walk_task=walk_task, include_size=track_bytes,
            )
            # Signal workers that listing is done.
            for _ in range(max_concurrent):
                await queue.put(None)

        sem = asyncio.Semaphore(max_concurrent)
        copy_task = self._progress.add_task(
            f"[cyan]Copying {label}[/cyan]", total=None, unit="files",
        )
        loop = asyncio.get_running_loop()
        total_bytes = 0
        done_count = 0
        start = loop.time()
        last_monitor_time = start

        async def _worker():
            nonlocal total_bytes, done_count, last_monitor_time
            nonlocal skipped, skipped_bytes
            while True:
                item = await queue.get()
                if item is None:
                    return
                remote_file, local_file, remote_size = item

                # Resume: skip files whose local size matches remote.
                if remote_size is not None and os.path.isfile(local_file):
                    try:
                        local_size = os.path.getsize(local_file)
                    except OSError:
                        local_size = -1
                    if local_size == remote_size:
                        skipped += 1
                        skipped_bytes += remote_size
                        continue

                async with sem:
                    while True:
                        try:
                            if use_s3:
                                data = await self.send(
                                    f"{target}.get_file", remote_file,
                                    use_s3=True, s3_prefix=s3_prefix)
                            else:
                                data = await self.send(
                                    f"{target}.get_file", remote_file,
                                    chunk_size=chunk_size)
                            break
                        except (PeerNotFoundError, ConnectionError,
                                asyncio.TimeoutError) as exc:
                            _logger.warning(
                                "Transfer of %s failed (%s), retrying in %.1fs \u2026",
                                os.path.basename(remote_file), exc,
                                self.peer_delay,
                            )
                            await self.monitor(
                                f"{self.name}: transfer of "
                                f"{os.path.basename(remote_file)} failed "
                                f"({type(exc).__name__}), retrying \u2026",
                                status="warning",
                            )
                            await asyncio.sleep(self.peer_delay)

                    os.makedirs(os.path.dirname(local_file), exist_ok=True)
                    file_size = (os.path.getsize(data) if isinstance(data, str)
                                 else len(data))
                    # Skip writing if the local file already has the same size.
                    skip = False
                    if os.path.isfile(local_file):
                        try:
                            local_size = os.path.getsize(local_file)
                        except OSError:
                            local_size = -1
                        if local_size == file_size:
                            skip = True
                            _logger.debug("Skipping %s (local size matches)",
                                          os.path.basename(local_file))
                            if isinstance(data, str):
                                os.unlink(data)
                    if not skip:
                        if isinstance(data, str):
                            # S3 download returned a temp file path — move it.
                            await loop.run_in_executor(
                                None, shutil.move, data, local_file)
                        else:
                            await loop.run_in_executor(
                                None, _write_file, local_file, data)
                    total_bytes += file_size
                    done_count += 1
                    self._progress.update(copy_task, completed=done_count)

                    # Send periodic progress to monitor (at most every 30s).
                    now = loop.time()
                    if now - last_monitor_time >= 30:
                        last_monitor_time = now
                        elapsed = now - start
                        rate = total_bytes / elapsed if elapsed > 0 else 0
                        await self.monitor(
                            f"{self.name}: {done_count} files "
                            f"({_fmt_binary(total_bytes)}, "
                            f"{_fmt_binary(rate)}/s)",
                            status="progress",
                        )

        walk_coro = _walk_and_enqueue()
        workers = [_worker() for _ in range(max_concurrent)]
        await asyncio.gather(walk_coro, *workers)

        self._progress.remove_task(walk_task)
        self._progress.remove_task(copy_task)

        if skipped:
            _logger.info(
                "Skipped %d already-complete file(s) (%s)",
                skipped, _fmt_binary(skipped_bytes),
            )
            self._progress.console.print(
                f"Skipped [bold]{skipped}[/bold] already-complete file(s) "
                f"([bold]{_fmt_binary(skipped_bytes)}[/bold])"
            )

        elapsed = loop.time() - start
        rate = total_bytes / elapsed if elapsed > 0 else 0
        summary = (
            f"Transferred {_fmt_binary(total_bytes)} "
            f"in {elapsed:.1f}s ({_fmt_binary(rate)}/s)"
        )
        self._progress.console.print(
            f"Transferred [bold]{_fmt_binary(total_bytes)}[/bold] "
            f"in [bold]{elapsed:.1f}s[/bold] "
            f"([bold]{_fmt_binary(rate)}/s[/bold])"
        )
        await self.monitor(f"{self.name}: {summary}", status="ok")

    async def _walk_remote(self, target, remote_path, local_path, file_list,
                            walk_task=None, include_size=False):
        os.makedirs(local_path, exist_ok=True)
        entries = []
        offset = 0
        limit = 10000
        while True:
            while True:
                try:
                    page = await self.send(f"{target}.list_dir", remote_path,
                                           include_size=include_size,
                                           offset=offset, limit=limit)
                    break
                except (PeerNotFoundError, ConnectionError,
                        asyncio.TimeoutError) as exc:
                    _logger.warning(
                        "Listing %s failed (%s), retrying in %.1fs …",
                        remote_path, exc, self.peer_delay,
                    )
                    await self.monitor(
                        f"{self.name}: listing {remote_path} failed "
                        f"({type(exc).__name__}), retrying …",
                        status="warning",
                    )
                    await asyncio.sleep(self.peer_delay)
            entries.extend(page)
            if len(page) < limit:
                break
            offset += len(page)
        for entry in entries:
            name = entry["name"]
            remote_child = f"{remote_path}/{name}" if remote_path != "." else name
            local_child = os.path.join(local_path, name)
            if entry["type"] == "dir":
                await self._walk_remote(target, remote_child, local_child,
                                         file_list, walk_task=walk_task,
                                         include_size=include_size)
            else:
                remote_size = entry.get("size")
                file_list.append((remote_child, local_child, remote_size))
                if walk_task is not None:
                    self._progress.update(walk_task, completed=len(file_list))

    async def _walk_remote_streamed(self, target, remote_path, local_path,
                                     queue, walk_task=None,
                                     include_size=False, _counter=None):
        """Walk a remote directory and push files onto *queue* as discovered.

        Unlike ``_walk_remote``, this feeds files to the queue page-by-page
        so that downloads can start while listing is still in progress.
        """
        if _counter is None:
            _counter = [0]
        os.makedirs(local_path, exist_ok=True)
        offset = 0
        limit = 10000
        while True:
            while True:
                try:
                    page = await self.send(f"{target}.list_dir", remote_path,
                                           include_size=include_size,
                                           offset=offset, limit=limit)
                    break
                except (PeerNotFoundError, ConnectionError,
                        asyncio.TimeoutError) as exc:
                    _logger.warning(
                        "Listing %s failed (%s), retrying in %.1fs …",
                        remote_path, exc, self.peer_delay,
                    )
                    await self.monitor(
                        f"{self.name}: listing {remote_path} failed "
                        f"({type(exc).__name__}), retrying …",
                        status="warning",
                    )
                    await asyncio.sleep(self.peer_delay)

            dirs = []
            for entry in page:
                name = entry["name"]
                remote_child = (f"{remote_path}/{name}"
                                if remote_path != "." else name)
                local_child = os.path.join(local_path, name)
                if entry["type"] == "dir":
                    dirs.append((remote_child, local_child))
                else:
                    remote_size = entry.get("size")
                    _counter[0] += 1
                    if walk_task is not None:
                        self._progress.update(
                            walk_task, completed=_counter[0])
                    await queue.put((remote_child, local_child, remote_size))

            if len(page) < limit:
                break
            offset += len(page)

        for remote_child, local_child in dirs:
            await self._walk_remote_streamed(
                target, remote_child, local_child, queue,
                walk_task=walk_task, include_size=include_size,
                _counter=_counter,
            )

    # ------------------------------------------------------------------
    # S3 staging (sending side)
    # ------------------------------------------------------------------

    def _s3_cleanup(self, s3_key: str):
        """Delete an S3 object that this client previously uploaded.

        Only keys uploaded by this client instance are accepted, to prevent
        a peer from deleting arbitrary objects in the shared bucket.
        """
        if s3_key not in self._s3_keys:
            raise PermissionError(f"unknown S3 key: {s3_key}")
        from nexus_transfers import s3 as _s3

        _s3.delete(s3_key)
        self._s3_keys.discard(s3_key)
        return True

    async def _upload_and_reply(self, sender: str, msg_id: str,
                                transfer: S3Transfer):
        """Upload the file backing ``transfer`` and send the S3 reply frame."""
        from nexus_transfers import s3 as _s3

        loop = asyncio.get_running_loop()
        fname = os.path.basename(transfer.local_path)
        size = os.path.getsize(transfer.local_path)
        task_id = self._progress.add_task(f"↑ {fname} (s3)", total=size)

        def _on_progress(n: int) -> None:
            self._progress.advance(task_id, n)

        try:
            bucket, s3_key, real_size, checksum = await loop.run_in_executor(
                None, _s3.upload_file, transfer.local_path, _on_progress,
                transfer.s3_prefix,
            )
        except Exception as exc:
            self._progress.remove_task(task_id)
            body = {"msg_id": msg_id,
                    "error": f"{type(exc).__name__}: {exc}",
                    "traceback": traceback.format_exc()}
            await self._ws.send(encode_frame(self.name, "reply", sender, "J",
                                             json.dumps(body).encode()))
            return

        self._progress.remove_task(task_id)
        self._s3_keys.add(s3_key)
        body = {
            "msg_id": msg_id,
            "result": {"s3_key": s3_key, "size": real_size,
                       "checksum": checksum, "bucket": bucket},
            "s3_transfer": True,
        }
        await self._ws.send(encode_frame(self.name, "reply", sender, "J",
                                         json.dumps(body).encode()))

    # ------------------------------------------------------------------
    # S3 staging (receiving side)
    # ------------------------------------------------------------------

    async def _handle_s3_download(self, msg_id: str, source: str,
                                  info: dict):
        """Download an S3-staged file then ask the source to delete it.

        Resolves the original ``send`` future with the temp file path once
        the download completes and the checksum verifies.
        """
        from nexus_transfers import s3 as _s3

        s3_key = info.get("s3_key")
        size = int(info.get("size", 0))
        checksum = info.get("checksum")
        bucket = info.get("bucket")
        fname = s3_key.rsplit("/", 1)[-1] if s3_key else "file"
        task_id = self._progress.add_task(f"↓ {fname} (s3)", total=size)
        loop = asyncio.get_running_loop()

        def _on_progress(n: int) -> None:
            self._progress.advance(task_id, n)

        try:
            data = await loop.run_in_executor(
                None, _s3.download_file, s3_key, checksum, _on_progress,
                None, bucket,
            )
        except Exception as exc:
            self._progress.remove_task(task_id)
            future = self._pending.pop(msg_id, None)
            if future and not future.done():
                future.set_exception(RemoteError(f"S3 download failed: {exc}"))
            asyncio.create_task(self._s3_cleanup_remote(source, s3_key))
            return

        self._progress.remove_task(task_id)
        future = self._pending.pop(msg_id, None)
        if future and not future.done():
            future.set_result(data)
        asyncio.create_task(self._s3_cleanup_remote(source, s3_key))

    async def _s3_cleanup_remote(self, target: str, s3_key: str | None):
        """Tell ``target`` to delete ``s3_key``; log on failure but don't raise."""
        if not s3_key:
            return
        try:
            await self.send(f"{target}.s3_cleanup", s3_key)
        except Exception as exc:
            _logger.warning("S3 cleanup failed for %s: %s", s3_key, exc)

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
            if func_name == "list_dir":
                path_arg = func_args[0] if func_args else "?"
                _logger.info("Listing directory %s for %s …", path_arg, sender)
                await self.monitor(
                    f"{self.name}: listing {path_arg} for {sender} …",
                    status="progress",
                )

            # Run in an executor so the event loop stays responsive
            # (e.g. answering WebSocket pings) while slow I/O-bound
            # functions like list_dir are executing.
            loop = asyncio.get_running_loop()
            if func_name == "list_dir":
                label = os.path.basename(
                    (func_args[0] if func_args else "?").rstrip("/")
                ) or func_args[0]
                task_id = self._progress.add_task(
                    f"[magenta]Listing {label}[/magenta]",
                    total=None, unit="files",
                )

                def _on_progress(count):
                    self._progress.update(task_id, completed=count)

                func_kwargs["progress_callback"] = _on_progress

            if isinstance(func_args, list):
                result = await loop.run_in_executor(
                    None, lambda: func(*func_args, **func_kwargs))
            else:
                result = await loop.run_in_executor(
                    None, lambda: func(func_args, **func_kwargs))

            if func_name == "list_dir":
                self._progress.remove_task(task_id)
                count = len(result) if isinstance(result, list) else 0
                _logger.info("Listed %d entries in %s for %s",
                             count, path_arg, sender)
                await self.monitor(
                    f"{self.name}: listed {count} entries in {path_arg} "
                    f"for {sender}",
                    status="progress",
                )
        except Exception as exc:
            if func_name == "list_dir":
                self._progress.remove_task(task_id)
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
        elif isinstance(result, S3Transfer):
            asyncio.create_task(self._upload_and_reply(sender, msg_id, result))
        else:
            body = {"msg_id": msg_id, "result": result}
            await self._ws.send(encode_frame(self.name, "reply", sender, "J",
                                             json.dumps(body).encode()))

    # ------------------------------------------------------------------
    # Background listener (headless / programmatic mode)
    # ------------------------------------------------------------------

    async def _listener(self):
        """Background task that handles incoming frames.

        When the WebSocket connection drops unexpectedly, attempts to
        reconnect according to ``reconnect_retries`` / ``reconnect_delay``.
        """
        while True:
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
                            elif data.get("s3_transfer"):
                                asyncio.create_task(
                                    self._handle_s3_download(msg_id, source,
                                                              data.get("result", {}))
                                )
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
                                if "unknown target" in error:
                                    future.set_exception(PeerNotFoundError(error))
                                else:
                                    future.set_exception(RemoteError(error))

                        case _:
                            _logger.debug("[recv] unhandled msg_name=%r from %s", msg_name, source)

                # The async for loop ended normally — clean disconnect.
                if self._closed:
                    return
                _logger.warning("Connection closed by server")
                for future in self._pending.values():
                    if not future.done():
                        future.set_exception(ConnectionError("connection closed"))
                self._pending.clear()
                self._binary_buffers.clear()
                self._binary_received.clear()
                self._binary_hashes.clear()
                self._binary_checksums.clear()
                try:
                    await self._reconnect()
                except (NameTakenError, ConnectionError):
                    raise
                except Exception:
                    _logger.error(
                        "Listener exiting: reconnection failed after "
                        "clean disconnect", exc_info=True,
                    )
                    return

            except asyncio.CancelledError:
                return
            except Exception as exc:
                if self._closed:
                    return
                # Close code 1009 = message too big — do not reconnect
                # (reconnecting would reproduce the same oversized reply).
                if (
                    isinstance(exc, ConnectionClosedError)
                    and exc.rcvd is not None
                    and exc.rcvd.code == 1009
                ):
                    _logger.error(
                        "Connection closed: message too big (1009). "
                        "Increase max_size or reduce payload."
                    )
                    for future in self._pending.values():
                        if not future.done():
                            future.set_exception(
                                ConnectionError("message too big (1009)")
                            )
                    self._pending.clear()
                    self._binary_buffers.clear()
                    self._binary_received.clear()
                    self._binary_hashes.clear()
                    self._binary_checksums.clear()
                    raise
                _logger.warning("Connection lost: %s", exc)
                # Fail all pending futures so callers don't hang.
                for future in self._pending.values():
                    if not future.done():
                        future.set_exception(ConnectionError("connection lost"))
                self._pending.clear()
                self._binary_buffers.clear()
                self._binary_received.clear()
                self._binary_hashes.clear()
                self._binary_checksums.clear()
                try:
                    await self._reconnect()
                except (NameTakenError, ConnectionError):
                    raise
                except Exception:
                    _logger.error(
                        "Listener exiting: reconnection failed", exc_info=True,
                    )
                    return


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
    parser.add_argument("--reconnect-retries", type=int, default=-1,
                        help="Reconnection attempts on disconnect (-1 = infinite, default: -1)")
    parser.add_argument("--reconnect-delay", type=float, default=2.0,
                        help="Seconds between reconnection attempts (default: 2.0)")
    parser.add_argument("--peer-retries", type=int, default=-1,
                        help="Retries when target peer is not found (-1 = infinite, default: -1)")
    parser.add_argument("--peer-delay", type=float, default=2.0,
                        help="Seconds between peer-not-found retries (default: 2.0)")
    parser.add_argument("--call-timeout", type=float, default=None,
                        help="Timeout in seconds for RPC calls (default: no timeout)")
    parser.add_argument("--no-verify", action="store_true",
                        help="Skip TLS certificate verification for wss:// connections")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    from rich.logging import RichHandler
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        handlers=[RichHandler(rich_tracebacks=True)],
    )
    client_kwargs = dict(
        reconnect_retries=args.reconnect_retries,
        reconnect_delay=args.reconnect_delay,
        peer_retries=args.peer_retries,
        peer_delay=args.peer_delay,
        call_timeout=args.call_timeout,
        ssl_verify=not args.no_verify,
    )
    coro = (
        _interactive(args.name, args.server_url,
                     allowed_paths=args.allow_path or None, **client_kwargs)
        if args.interactive
        else _serve(args.name, args.server_url,
                    allowed_paths=args.allow_path or None, **client_kwargs)
    )
    asyncio.run(coro)


if __name__ == "__main__":
    main()
