"""WebSocket relay server that routes binary frames between named clients.

The server only decodes the JSON payload of frames addressed to itself
(target == "").  All other frames are forwarded verbatim so the server
never needs to inspect client-to-client message bodies.
"""

import asyncio
import json
import logging
import threading

from websockets.asyncio.server import serve

from nexus_transfers.protocol import decode_frame, encode_frame

logger = logging.getLogger(__name__)

# name -> websocket mapping
clients: dict[str, object] = {}
clients_lock = threading.Lock()  # kept; conftest.py uses it to clear state between tests


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
                continue

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
                await target_ws.send(raw)
                logger.debug("relay %s -> %s: %s (%d bytes)",
                             client_name, target, msg_name, len(raw))
                continue

            # ----------------------------------------------------------------
            # Frame addressed to the server — decode JSON and dispatch
            # ----------------------------------------------------------------
            match msg_name:
                case "register":
                    # Re-registration from an already-registered client
                    await websocket.send(_ok(client_name, "register"))

                case "list_clients":
                    with clients_lock:
                        names = sorted(clients.keys())
                    await websocket.send(_ok(client_name, "list_clients", {"clients": names}))
                    logger.debug("list_clients -> %s: %s", client_name, names)

                case msg:
                    await websocket.send(_err(client_name, f"unknown message '{msg}'"))

    except Exception as exc:
        logger.error("[!] Error with %s: %s", client_name or remote, exc)
    finally:
        if client_name:
            with clients_lock:
                clients.pop(client_name, None)
            logger.info("[-] Disconnected: %s (%s)", client_name, remote)
        else:
            logger.info("[-] Disconnected unregistered: %s", remote)


async def run_server(host="localhost", port=8766):
    """Start the relay server."""
    async with serve(relay_handler, host, port):
        print(f"Relay server listening on ws://{host}:{port}")
        await asyncio.get_running_loop().create_future()


def main():
    """CLI entry point for ``nexus-server``."""
    import argparse

    parser = argparse.ArgumentParser(description="Transfer relay server")
    parser.add_argument("--host", default="localhost", help="Bind address")
    parser.add_argument("--port", type=int, default=8766, help="Bind port")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    from rich.logging import RichHandler
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        handlers=[RichHandler(rich_tracebacks=True)],
    )
    asyncio.run(run_server(args.host, args.port))


if __name__ == "__main__":
    main()
