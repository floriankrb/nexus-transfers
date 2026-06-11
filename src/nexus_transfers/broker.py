"""WebSocket relay broker that routes binary frames between named clients.

The broker only decodes the JSON payload of frames addressed to itself
(target == "").  All other frames are forwarded verbatim so the broker
never needs to inspect client-to-client message bodies.
"""

import asyncio
import json
import logging
import threading
from datetime import datetime, timezone

from websockets.asyncio.server import serve

from nexus_transfers.protocol import decode_frame, encode_frame

logger = logging.getLogger(__name__)

# name -> websocket mapping
clients: dict[str, object] = {}
clients_lock = threading.Lock()  # kept; conftest.py uses it to clear state between tests

# name -> websocket mapping for monitor clients
monitors: dict[str, object] = {}
monitors_lock = threading.Lock()

# Pending calls: target_name -> list of (msg_id, source_name)
# Tracks calls relayed to a target that have not yet been replied to.
pending_calls: dict[str, list[tuple[str, str]]] = {}
pending_calls_lock = threading.Lock()

# Clients already warned about for sending frames with a spoofed source.
# Peers are trusted: the frame is still relayed, but the mismatch is logged
# once per client so it is visible without flooding the logs.
_spoof_warned: set[str] = set()


def _utcnow() -> str:
    """Return current UTC time in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


async def _broadcast_event(event: dict):
    """Send an event to all registered monitors (fire-and-forget, no reply)."""
    payload = json.dumps(event).encode()
    frame = encode_frame("", "monitor_event", "", "J", payload)
    with monitors_lock:
        targets = list(monitors.values())
    for ws in targets:
        try:
            await ws.send(frame)
        except Exception:
            pass  # monitor disconnected; will be cleaned up on close


def _err(target: str, error: str, *, msg_id: str | None = None) -> bytes:
    body: dict = {"error": error}
    if msg_id is not None:
        body["msg_id"] = msg_id
    return encode_frame("", "error", target, "J", json.dumps(body).encode())


def _ok(target: str, msg_name: str, body: dict | None = None) -> bytes:
    return encode_frame("", msg_name, target, "J", json.dumps(body or {}).encode())


async def relay_handler(websocket):
    """Handle a single client connection."""
    client_name = None
    is_monitor = False
    remote = websocket.remote_address

    try:
        async for raw in websocket:
            if not isinstance(raw, bytes):
                continue

            try:
                version, source, msg_name, target, encoding, payload = decode_frame(raw)
            except ValueError:
                continue

            if version != 1:
                await websocket.send(_err(source or "", "unsupported protocol version"))
                continue

            # ----------------------------------------------------------------
            # Not yet registered — first frame must be "register"
            # ----------------------------------------------------------------
            if client_name is None:
                if msg_name != "register" or not source:
                    await websocket.send(_err(source or "", "must register first"))
                    continue
                with clients_lock:
                    if source in clients:
                        await websocket.send(_err(source, f"name '{source}' already taken"))
                        continue
                    clients[source] = websocket
                client_name = source
                await websocket.send(_ok(client_name, "register"))
                logger.info("[+] Registered: %s (%s)", client_name, remote)
                # Emit connect event to monitors
                await _broadcast_event({
                    "type": "connected",
                    "date": _utcnow(),
                    "source": client_name,
                    "message": f"Client '{client_name}' connected",
                })
                continue

            # Peers are trusted, but make spoofed source names visible
            # (one warning per offending client, frames relayed unchanged).
            if source != client_name and client_name not in _spoof_warned:
                _spoof_warned.add(client_name)
                logger.warning(
                    "Client '%s' sent a frame claiming source '%s' — "
                    "frames are relayed as-is (peers are trusted)",
                    client_name, source,
                )

            # ----------------------------------------------------------------
            # Route to another client without decoding the payload
            # ----------------------------------------------------------------
            if target:
                with clients_lock:
                    target_ws = clients.get(target)
                if target_ws is None:
                    # Peek at msg_id only when the payload is JSON so callers
                    # can correlate the error with their pending future.
                    msg_id = None
                    if encoding == "J":
                        try:
                            msg_id = json.loads(payload).get("msg_id")
                        except Exception:
                            pass
                    await websocket.send(_err(client_name, f"unknown target '{target}'", msg_id=msg_id))
                    continue
                try:
                    await target_ws.send(raw)
                except Exception as send_exc:
                    logger.warning(
                        "Relay %s -> %s failed: %s",
                        client_name, target, send_exc,
                    )
                    msg_id = None
                    if encoding == "J":
                        try:
                            msg_id = json.loads(payload).get("msg_id")
                        except Exception:
                            pass
                    await websocket.send(
                        _err(client_name, f"target '{target}' disconnected",
                             msg_id=msg_id)
                    )
                    continue

                # Track pending calls/replies for dead-peer detection
                if encoding == "J" and msg_name in ("call", "reply"):
                    try:
                        msg_id = json.loads(payload).get("msg_id")
                    except Exception:
                        msg_id = None
                    if msg_id:
                        if msg_name == "call":
                            with pending_calls_lock:
                                pending_calls.setdefault(target, []).append(
                                    (msg_id, client_name)
                                )
                        elif msg_name == "reply":
                            with pending_calls_lock:
                                entries = pending_calls.get(client_name, [])
                                pending_calls[client_name] = [
                                    e for e in entries if e[0] != msg_id
                                ]

                logger.debug("relay %s -> %s: %s (%d bytes)",
                             client_name, target, msg_name, len(raw))
                continue

            # ----------------------------------------------------------------
            # Frame addressed to the broker — decode JSON and dispatch
            # ----------------------------------------------------------------
            match msg_name:
                case "register":
                    # Re-registration from an already-registered client
                    await websocket.send(_ok(client_name, "register"))

                case "register_monitor":
                    with monitors_lock:
                        monitors[client_name] = websocket
                    is_monitor = True
                    await websocket.send(_ok(client_name, "register_monitor"))
                    logger.info("[+] Monitor registered: %s", client_name)

                case "list_clients":
                    with clients_lock:
                        names = sorted(clients.keys())
                    await websocket.send(_ok(client_name, "list_clients", {"clients": names}))
                    logger.debug("list_clients -> %s: %s", client_name, names)

                case "monitor_event":
                    # Client emitting a monitoring event — broadcast to all monitors
                    try:
                        event = json.loads(payload)
                    except (json.JSONDecodeError, ValueError):
                        continue
                    # Ensure source is set
                    event.setdefault("source", client_name)
                    event.setdefault("date", _utcnow())
                    await _broadcast_event(event)

                case msg:
                    await websocket.send(_err(client_name, f"unknown message '{msg}'"))

    except Exception as exc:
        logger.error("[!] Error with %s: %s", client_name or remote, exc)
    finally:
        if client_name:
            with clients_lock:
                clients.pop(client_name, None)
            if is_monitor:
                with monitors_lock:
                    monitors.pop(client_name, None)

            # Send error replies for any calls that this client never answered
            with pending_calls_lock:
                orphaned = pending_calls.pop(client_name, [])
            for msg_id, caller in orphaned:
                with clients_lock:
                    caller_ws = clients.get(caller)
                if caller_ws is not None:
                    try:
                        await caller_ws.send(
                            _err(caller,
                                 f"peer '{client_name}' disconnected before replying",
                                 msg_id=msg_id)
                        )
                    except Exception:
                        pass  # caller also gone

            logger.info("[-] Disconnected: %s (%s)", client_name, remote)
            # Emit disconnect event to monitors
            await _broadcast_event({
                "type": "disconnected",
                "date": _utcnow(),
                "source": client_name,
                "message": f"Client '{client_name}' disconnected",
            })
        else:
            logger.info("[-] Disconnected unregistered: %s", remote)


async def run_broker(host="localhost", port=8766):
    """Start the relay broker."""
    async with serve(relay_handler, host, port, max_size=10_485_760):  # 10 MiB
        print(f"Relay broker listening on ws://{host}:{port}")
        await asyncio.get_running_loop().create_future()


def main():
    """CLI entry point for ``nexus-broker``."""
    import argparse

    from nexus_transfers.config import cli_default

    parser = argparse.ArgumentParser(description="Transfer relay broker")
    parser.add_argument("--host",
                        default=cli_default("host", "broker", default="localhost"),
                        help="Bind address")
    parser.add_argument("--port", type=int,
                        default=cli_default("port", "broker", default=8766, type_fn=int),
                        help="Bind port")
    parser.add_argument("--debug", action="store_true",
                        default=cli_default("debug", "broker", default=False),
                        help="Enable debug logging")
    args = parser.parse_args()
    from rich.logging import RichHandler
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        handlers=[RichHandler(rich_tracebacks=True)],
    )
    asyncio.run(run_broker(args.host, args.port))


if __name__ == "__main__":
    main()
