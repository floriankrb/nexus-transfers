"""WebSocket RPC client – the core Client class."""

import asyncio
import base64
import hashlib
import json
import logging
import os
import ssl
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
)
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosedError

from nexus_transfers._progress import _BinarySpeedColumn, _CountOrBytesColumn
from nexus_transfers.config import resolve
from nexus_transfers.dispatch import (
    DISPATCH,
    FileTransfer,
    S3Transfer,
    make_get_file,
    make_list_dir,
)
from nexus_transfers.protocol import decode_frame, encode_frame

from ._errors import NameTakenError, PeerNotFoundError, RemoteError
from ._transfer import _DirectoryTransfer

load_dotenv(Path.home() / ".env")

_logger = logging.getLogger(__name__)
_DEFAULT_URL = resolve("NEXUS_TRANSFERS_URL", default="ws://localhost:8766")


class Client:
    """Programmatic RPC client that connects to the relay broker.

    Parameters
    ----------
    name:
        Unique client identifier.
    url:
        WebSocket broker URL.
    dispatch:
        Dispatch table for incoming calls.  Defaults to the built-in
        ``DISPATCH`` table (adder, echo).
    allowed_paths:
        List of directories that ``get_file`` and ``list_dir`` may access.
        If ``None``, file operations are disabled.
    reconnect_retries:
        Number of reconnection attempts when the broker connection drops.
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
    on_monitor_event:
        Optional callable invoked with each monitor-event dict broadcast by
        the relay.  The client must also call :meth:`register_monitor` to
        subscribe to broadcasts; ``on_monitor_event`` only sets the handler.
        Equivalent to setting :attr:`on_monitor_event` after construction.
    """

    _MONITOR_TIMEOUT = 0.5

    def __init__(self, name, url=None, dispatch=None, allowed_paths=None,
                 reconnect_retries=0, reconnect_delay=2.0,
                 peer_retries=0, peer_delay=2.0,
                 call_timeout=None, ssl_verify=True,
                 on_monitor_event=None):
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
        self.on_monitor_event = on_monitor_event
        self._s3_keys: set[str] = set()
        if self.allowed_paths:
            self.dispatch["get_file"] = make_get_file(self.allowed_paths)
            self.dispatch["list_dir"] = make_list_dir(self.allowed_paths)
            self.dispatch["s3_cleanup"] = self._s3_cleanup
        self._ws = None
        self._pending: dict[str, asyncio.Future] = {}
        self._local_targets: dict[str, str] = {}
        self._binary_buffers: dict = {}
        self._binary_received: dict = {}
        self._binary_hashes: dict = {}
        self._binary_checksums: dict = {}
        self._reconnect_lock = asyncio.Lock()
        self._connected = asyncio.Event()
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
        self._is_monitor = False
        # Frames relayed by the broker before the registration ack arrived;
        # drained by the listener (see _register).
        self._early_frames: list[bytes] = []
        self._warned_plaintext_auth = False
        self._warned_no_tls_verify = False

    # ======================================================================
    # Connection lifecycle
    # ======================================================================

    async def _register(self):
        """Send a register frame and wait for the acknowledgement."""
        extra_headers = {}
        user = resolve("NEXUS_TRANSFERS_USER")
        password = resolve("NEXUS_TRANSFERS_PASSWORD")
        if user and password:
            credentials = base64.b64encode(f"{user}:{password}".encode()).decode()
            extra_headers["Authorization"] = f"Basic {credentials}"
            if not self.url.startswith("wss://") and not self._warned_plaintext_auth:
                self._warned_plaintext_auth = True
                _logger.warning(
                    "Sending basic-auth credentials over an unencrypted "
                    "connection (%s) — use wss:// in production", self.url,
                )

        _logger.debug("Connecting to %s as '%s'", self.url, self.name)
        connect_kwargs: dict = {
            "additional_headers": extra_headers,
            "max_size": 10_485_760,  # 10 MiB
            "ping_interval": 30,
            "ping_timeout": 60,
            "close_timeout": 10,
        }
        if not self.ssl_verify and self.url.startswith("wss://"):
            if not self._warned_no_tls_verify:
                self._warned_no_tls_verify = True
                _logger.warning(
                    "TLS certificate verification is disabled for %s", self.url,
                )
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            connect_kwargs["ssl"] = ssl_context
        self._ws = await connect(self.url, **connect_kwargs).__aenter__()

        reg = encode_frame(self.name, "register", "", "J", b"{}")
        await self._ws.send(reg)

        # The broker publishes the name to its routing table before sending
        # the ack, so a frame relayed from another client can arrive ahead
        # of the ack.  Buffer such frames for the listener instead of
        # misreading them as the registration response.
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 30
        while True:
            ack = await asyncio.wait_for(
                self._ws.recv(), timeout=max(deadline - loop.time(), 0.1))
            if not isinstance(ack, bytes):
                continue
            try:
                _, _src, msg_name, _tgt, _enc, payload = decode_frame(ack)
            except ValueError:
                continue
            if msg_name == "register":
                break
            if msg_name == "error":
                try:
                    body = json.loads(payload)
                except (json.JSONDecodeError, ValueError):
                    body = {}
                if body.get("msg_id"):
                    # Error for an in-flight call, not for the registration.
                    self._early_frames.append(ack)
                    continue
                error_msg = body.get("error", "")
                if "already taken" in error_msg:
                    raise NameTakenError(error_msg)
                raise RuntimeError(f"Registration failed: {error_msg}")
            self._early_frames.append(ack)
        self._connected.set()

    async def connect(self):
        """Connect to the broker and register, with optional retries."""
        await self._register()
        self._progress.start()
        self._listener_task = asyncio.create_task(self._listener())
        await self.monitor(
            f"{self.name}: connected to {self.url}",
            status="ok",
        )

    async def register_monitor(self, callback=None):
        """Register this client as a monitor to receive broadcast events.

        Parameters
        ----------
        callback
            Callable invoked with each event dict when a monitor_event
            frame arrives. If None, events are silently discarded unless
            _on_monitor_event was set directly.
        """
        if callback is not None:
            self.on_monitor_event = callback
        self._is_monitor = True
        frame = encode_frame(self.name, "register_monitor", "", "J", b"{}")
        await self._ws.send(frame)
        # Wait for ack
        ack_future = asyncio.get_running_loop().create_future()
        self._pending["__register_monitor__"] = ack_future
        await asyncio.wait_for(ack_future, timeout=10)

    async def close(self):
        """Disconnect from the broker."""
        self._closed = True
        self._connected.set()
        if self._ws is not None:
            await self.monitor(
                f"{self.name}: disconnecting from {self.url}",
                status="info",
            )
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

    async def __aexit__(self, exc_type, exc_value, traceback):
        if exc_type is not None and self._ws is not None:
            try:
                await self.monitor(
                    f"{self.name}: failed "
                    f"({exc_type.__name__}: {exc_value})",
                    status="error",
                )
            except Exception as monitor_exc:
                _logger.warning(
                    "Failed to send error monitor message: %s", monitor_exc,
                )
        await self.close()

    # ======================================================================
    # Reconnection
    # ======================================================================

    async def _reconnect(self):
        """Attempt to re-establish the broker connection.

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
                if self._is_monitor:
                    # Restore the monitor subscription lost with the old
                    # connection.  Fire-and-forget: the ack cannot be awaited
                    # here because _reconnect runs inside the listener task,
                    # which is the only consumer of incoming frames.
                    await self._ws.send(
                        encode_frame(self.name, "register_monitor", "", "J", b"{}")
                    )
                _logger.info("Reconnected successfully")
                await self.monitor(
                    f"{self.name}: reconnected to {self.url} "
                    f"after {attempt} attempt(s)",
                    status="ok",
                )
                return
            except NameTakenError:
                raise
            except Exception as exc:
                _logger.warning("Reconnection attempt %d failed: %s", attempt, exc)

    async def _safe_send(self, frame: bytes) -> None:
        """Send a frame, waiting for ``_listener`` to reconnect on failure.

        Retries up to ``reconnect_retries`` times after the initial attempt
        (``-1`` means infinite). Raises ``ConnectionError`` if the client has
        been closed or the retry budget is exhausted.

        Intended for use inside fire-and-forget Tasks (``_upload_and_reply``,
        ``_send_file_chunks``) whose frames would otherwise be lost when the
        websocket dies mid-send.
        """
        attempt = 0
        while True:
            if self._closed:
                raise ConnectionError("client is closed")
            try:
                await self._ws.send(frame)
                return
            except ConnectionClosedError as exc:
                self._connected.clear()
                _logger.warning(
                    "send failed (%s); waiting for reconnect …", exc,
                )
                await self._connected.wait()
                if self._closed:
                    raise ConnectionError(
                        "client closed while waiting for reconnect",
                    ) from exc
                attempt += 1
                if (
                    self.reconnect_retries != -1
                    and attempt > self.reconnect_retries
                ):
                    raise ConnectionError("send retries exhausted") from exc

    @staticmethod
    def _log_task_exception(task: asyncio.Task) -> None:
        """Surface exceptions from fire-and-forget Tasks.

        Attached as a done-callback so a failure inside a background Task
        produces a single log line instead of asyncio's full
        ``Task exception was never retrieved`` traceback.
        """
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            _logger.warning(
                "Background task %s failed: %s", task.get_name(), exc,
            )

    # ======================================================================
    # Outgoing calls
    # ======================================================================

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

        local_target = kwargs.pop("_local_target", None)

        attempt = 0
        while True:
            target, func_name = target_func.split(".", 1)
            msg_id = str(uuid.uuid4())[:8]
            future = asyncio.get_running_loop().create_future()
            self._pending[msg_id] = future
            if local_target is not None:
                self._local_targets[msg_id] = local_target

            body = {"msg_id": msg_id, "func": func_name, "args": list(args)}
            if kwargs:
                body["kwargs"] = kwargs
            frame = encode_frame(self.name, "call", target, "J", json.dumps(body).encode())
            _logger.debug("[send] call %s -> %s.%s (id=%s)", self.name, target, func_name, msg_id)

            try:
                await asyncio.wait_for(self._ws.send(frame), timeout=30)
            except Exception as exc:
                self._pending.pop(msg_id, None)
                self._local_targets.pop(msg_id, None)
                raise ConnectionError(f"failed to send: {exc}") from exc

            try:
                if self.call_timeout is not None:
                    result = await asyncio.wait_for(future, self.call_timeout)
                else:
                    result = await future
                return result
            except asyncio.TimeoutError:
                self._pending.pop(msg_id, None)
                self._local_targets.pop(msg_id, None)
                raise
            except PeerNotFoundError:
                self._pending.pop(msg_id, None)
                self._local_targets.pop(msg_id, None)
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

    # ======================================================================
    # Monitoring
    # ======================================================================

    def _utcnow(self) -> str:
        """Return current UTC time in ISO 8601 format."""
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    async def _emit_event(self, event: dict):
        """Send a structured monitoring event to the broker for broadcast.

        Fire-and-forget: silently ignores failures so it never blocks
        the caller.
        """
        event.setdefault("source", self.name)
        event.setdefault("date", self._utcnow())
        payload = json.dumps(event).encode()
        frame = encode_frame(self.name, "monitor_event", "", "J", payload)
        try:
            await asyncio.wait_for(self._ws.send(frame), timeout=self._MONITOR_TIMEOUT)
        except Exception:
            _logger.debug("Monitor event send failed", exc_info=True)

    async def monitor(self, message: str, status: str | None = None,
                      event_type: str = "info", task: dict | None = None,
                      progress: dict | None = None):
        """Emit a monitoring event.

        Parameters
        ----------
        message
            Free-form message string.
        status
            Optional status label (e.g. ``"ok"``, ``"error"``, ``"progress"``).
            Maps to event_type if event_type not explicitly set.
        event_type
            Event type (e.g. ``"info"``, ``"error"``, ``"warning"``, ``"progress"``).
        task
            Optional task descriptor dict.
        progress
            Optional progress descriptor dict.
        """
        if status and event_type == "info":
            # Backwards compat: map status to event_type
            event_type = status

        event: dict = {
            "type": event_type,
            "message": message,
        }
        if task is not None:
            event["task"] = task
        if progress is not None:
            event["progress"] = progress
        await self._emit_event(event)

    # ======================================================================
    # Directory / file transfer helpers
    # ======================================================================

    async def get_directory(self, target, remote_path, local_path,
                            max_concurrent=4, chunk_size=65536,
                            use_s3=True, s3_prefix=None, track_bytes=False):
        """Recursively copy a remote directory to a local path.

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
            Binary chunk size in bytes (only used when ``use_s3`` is False).
        use_s3:
            If True (default), stage transfers through S3.
        s3_prefix:
            Optional prefix prepended to S3 keys for this transfer batch.
        track_bytes:
            If True, show progress in bytes and verify resume by size.
        """
        xfer = _DirectoryTransfer(
            client=self,
            target=target,
            remote_path=remote_path,
            local_path=local_path,
            max_concurrent=max_concurrent,
            chunk_size=chunk_size,
            use_s3=use_s3,
            s3_prefix=s3_prefix,
            track_bytes=track_bytes,
        )
        await xfer.run()

    # ======================================================================
    # S3 staging
    # ======================================================================

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

    async def _upload_and_reply(self, sender: str, msg_id: str, transfer):
        """Upload the file backing *transfer* and send the S3 reply frame."""
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
            await self._safe_send(encode_frame(self.name, "reply", sender, "J",
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
        await self._safe_send(encode_frame(self.name, "reply", sender, "J",
                                           json.dumps(body).encode()))

    async def _handle_s3_download(self, msg_id: str, source: str, info: dict):
        """Download an S3-staged file then ask the source to delete it.

        Resolves the original ``send`` future with the temp file path once
        the download completes and the checksum verifies.
        """
        from nexus_transfers import s3 as _s3

        s3_key = info["s3_key"]
        size = int(info["size"])
        checksum = info.get("checksum")
        bucket = info["bucket"]
        fname = s3_key.rsplit("/", 1)[-1] if s3_key else "file"
        task_id = self._progress.add_task(f"↓ {fname} (s3)", total=size)
        loop = asyncio.get_running_loop()

        def _on_progress(n: int) -> None:
            self._progress.advance(task_id, n)

        local_target = self._local_targets.pop(msg_id, None)

        try:
            data = await loop.run_in_executor(
                None,
                lambda: _s3.download_file(
                    s3_key,
                    target_path=local_target,
                    expected_checksum=checksum,
                    progress_callback=_on_progress,
                    bucket=bucket,
                ),
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
        """Tell *target* to delete *s3_key*; log on failure but don't raise."""
        if not s3_key:
            return
        try:
            await self.send(f"{target}.s3_cleanup", s3_key)
        except Exception as exc:
            _logger.warning("S3 cleanup failed for %s: %s", s3_key, exc)

    # ======================================================================
    # Binary chunk transfer
    # ======================================================================

    async def _send_file_chunks(self, target: str, msg_id: str, file_transfer):
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
                await self._safe_send(frame)
                self._progress.advance(task_id, len(chunk))
                await asyncio.sleep(0)
        self._progress.remove_task(task_id)

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

        # Reject malformed headers instead of letting an IndexError escape
        # into the listener, where it would be treated as a connection loss.
        if (
            not msg_id
            or not isinstance(chunk_idx, int)
            or not isinstance(total, int)
            or total < 1
            or not 0 <= chunk_idx < total
        ):
            _logger.warning("Ignoring malformed chunk header: %r", hdr)
            return None

        if msg_id not in self._binary_buffers:
            self._binary_buffers[msg_id] = [None] * total
            self._binary_received[msg_id] = 0
            self._binary_hashes[msg_id] = hashlib.sha256()
        buffer = self._binary_buffers[msg_id]
        if len(buffer) != total or buffer[chunk_idx] is not None:
            # total_chunks inconsistent with the first chunk, or duplicate
            # chunk index — ignore rather than corrupt the transfer state.
            _logger.warning(
                "Ignoring inconsistent chunk %d/%d for transfer %s",
                chunk_idx, total, msg_id,
            )
            return None
        buffer[chunk_idx] = chunk_data
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

    def _clear_buffers(self):
        """Fail all pending futures and clear per-transfer state."""
        for future in self._pending.values():
            if not future.done():
                future.set_exception(ConnectionError("connection lost"))
        self._pending.clear()
        self._binary_buffers.clear()
        self._binary_received.clear()
        self._binary_hashes.clear()
        self._binary_checksums.clear()
        self._local_targets.clear()
        self._early_frames.clear()
        for task_id in self._progress_task_ids.values():
            try:
                self._progress.remove_task(task_id)
            except Exception:
                pass
        self._progress_task_ids.clear()

    # ======================================================================
    # RPC dispatch and background listener
    # ======================================================================

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
            task = asyncio.create_task(self._send_file_chunks(sender, msg_id, result))
            task.add_done_callback(self._log_task_exception)
        elif isinstance(result, S3Transfer):
            task = asyncio.create_task(self._upload_and_reply(sender, msg_id, result))
            task.add_done_callback(self._log_task_exception)
        else:
            body = {"msg_id": msg_id, "result": result}
            await self._ws.send(encode_frame(self.name, "reply", sender, "J",
                                             json.dumps(body).encode()))

    async def _process_raw(self, raw) -> None:
        """Decode one raw websocket message and dispatch it safely.

        A malformed frame (bad JSON, inconsistent chunk header, …) is logged
        and dropped: it must not abort the connection and kill every other
        in-flight transfer.
        """
        if not isinstance(raw, bytes):
            return
        try:
            _, source, msg_name, _target, encoding, payload = decode_frame(raw)
        except ValueError:
            return

        _logger.debug("[recv] %s from %s", msg_name, source or "broker")

        try:
            await self._handle_frame(source, msg_name, payload)
        except Exception:
            _logger.warning(
                "Error handling %r frame from %s",
                msg_name, source or "broker", exc_info=True,
            )

    async def _handle_frame(self, source, msg_name: str, payload: bytes):
        """Handle one decoded incoming frame.

        Called from ``_listener``, which catches and logs any exception so
        that a single malformed frame cannot be mistaken for a connection
        loss.
        """
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
                    task = asyncio.create_task(
                        self._handle_s3_download(msg_id, source,
                                                 data.get("result", {}))
                    )
                    task.add_done_callback(self._log_task_exception)
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

            case "register_monitor":
                future = self._pending.pop("__register_monitor__", None)
                if future and not future.done():
                    future.set_result(True)

            case "monitor_event":
                if self.on_monitor_event is not None:
                    try:
                        event = json.loads(payload)
                        self.on_monitor_event(event)
                    except Exception:
                        _logger.debug("on_monitor_event failed", exc_info=True)

            case "error":
                data = json.loads(payload)
                error = data.get("error", "unknown error")
                msg_id = data.get("msg_id")
                future = self._pending.pop(msg_id, None)
                if future and not future.done():
                    if "unknown target" in error or "disconnected" in error:
                        future.set_exception(PeerNotFoundError(error))
                    else:
                        future.set_exception(RemoteError(error))

            case _:
                _logger.debug("[recv] unhandled msg_name=%r from %s", msg_name, source)

    async def _listener(self):
        """Background task that handles incoming frames.

        When the WebSocket connection drops unexpectedly, attempts to
        reconnect according to ``reconnect_retries`` / ``reconnect_delay``.
        """
        while True:
            try:
                # Frames the broker relayed before the registration ack
                # (buffered by _register) are handled first.
                while self._early_frames:
                    await self._process_raw(self._early_frames.pop(0))
                async for raw in self._ws:
                    await self._process_raw(raw)

                # The async for loop ended normally — clean disconnect.
                if self._closed:
                    return
                _logger.warning("Connection closed by broker")
                self._clear_buffers()
                self._connected.clear()
                try:
                    await self._reconnect()
                except (NameTakenError, ConnectionError):
                    self._closed = True
                    self._connected.set()
                    raise
                except Exception:
                    _logger.error(
                        "Listener exiting: reconnection failed after "
                        "clean disconnect", exc_info=True,
                    )
                    self._closed = True
                    self._connected.set()
                    return

            except asyncio.CancelledError:
                self._connected.set()
                return
            except Exception as exc:
                if self._closed:
                    self._connected.set()
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
                    self._clear_buffers()
                    self._closed = True
                    self._connected.set()
                    raise
                _logger.warning("Connection lost: %s", exc)
                self._clear_buffers()
                self._connected.clear()
                try:
                    await self._reconnect()
                except (NameTakenError, ConnectionError):
                    self._closed = True
                    self._connected.set()
                    raise
                except Exception:
                    _logger.error(
                        "Listener exiting: reconnection failed", exc_info=True,
                    )
                    self._closed = True
                    self._connected.set()
                    return
