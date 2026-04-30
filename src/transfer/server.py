"""WebSocket relay server that routes JSON messages between named clients."""

import asyncio
import json
import threading

from websockets.asyncio.server import serve

# Shared memory accessible by all client handler coroutines
shared_memory = {}
shared_memory_lock = threading.Lock()

# name -> websocket mapping
clients: dict[str, object] = {}
clients_lock = threading.Lock()


async def _send_error(websocket, error, **extra):
    """Send an error response to a client.

    Parameters
    ----------
    websocket
        The WebSocket connection.
    error
        Human-readable error description.
    **extra
        Additional fields to include in the response.
    """
    msg = {"status": "error", "error": error}
    msg.update(extra)
    await websocket.send(json.dumps(msg))


async def relay_handler(websocket):
    """Handle a single client connection.

    Parameters
    ----------
    websocket
        The WebSocket connection for this client.
    """
    client_name = None
    remote = websocket.remote_address

    try:
        async for raw_message in websocket:
            try:
                message = json.loads(raw_message)
            except (json.JSONDecodeError, TypeError):
                await _send_error(websocket, "invalid JSON")
                continue

            if not isinstance(message, dict) or "action" not in message:
                await _send_error(websocket, "missing 'action' field")
                continue

            action = message["action"]

            # --- Register ---
            if action == "register":
                name = message.get("name")
                if not name or not isinstance(name, str):
                    await _send_error(websocket, "missing or invalid 'name'")
                    continue
                with clients_lock:
                    if name in clients:
                        await _send_error(websocket, f"name '{name}' already taken")
                        continue
                    clients[name] = websocket
                client_name = name
                await websocket.send(json.dumps({"status": "ok", "action": "register", "name": name}))
                print(f"[+] Registered: {name}  ({remote})")
                continue

            # All subsequent actions require registration
            if client_name is None:
                await _send_error(websocket, "must register first")
                continue

            # --- Send message to a target ---
            if action == "send":
                target = message.get("to")
                msg_id = message.get("msg_id")
                payload = message.get("payload", {})
                if not target:
                    await _send_error(websocket, "missing 'to' field")
                    continue
                with clients_lock:
                    target_ws = clients.get(target)
                if target_ws is None:
                    await _send_error(websocket, f"unknown target '{target}'", msg_id=msg_id)
                    continue
                envelope = {
                    "action": "message",
                    "from": client_name,
                    "msg_id": msg_id,
                    "payload": payload,
                }
                await target_ws.send(json.dumps(envelope))
                print(f"    route {client_name} -> {target}: {payload}")
                continue

            # --- Reply to a message ---
            if action == "reply":
                target = message.get("to")
                msg_id = message.get("msg_id")
                payload = message.get("payload", {})
                if not target:
                    await _send_error(websocket, "missing 'to' field")
                    continue
                with clients_lock:
                    target_ws = clients.get(target)
                if target_ws is None:
                    await _send_error(websocket, f"unknown target '{target}'", msg_id=msg_id)
                    continue
                envelope = {
                    "action": "reply",
                    "from": client_name,
                    "msg_id": msg_id,
                    "payload": payload,
                }
                await target_ws.send(json.dumps(envelope))
                print(f"    reply {client_name} -> {target}: {payload}")
                continue

            # --- Shared-memory commands ---
            if action == "memory":
                cmd = message.get("cmd")
                if cmd == "set":
                    key, value = message["key"], message["value"]
                    with shared_memory_lock:
                        shared_memory[key] = value
                    await websocket.send(json.dumps({"status": "ok", "action": "memory", "cmd": "set", "key": key}))
                    print(f"    memory set: {key}={value!r}")
                elif cmd == "get":
                    key = message["key"]
                    with shared_memory_lock:
                        value = shared_memory.get(key)
                    await websocket.send(json.dumps({"status": "ok", "action": "memory", "cmd": "get", "key": key, "value": value}))
                elif cmd == "dump":
                    with shared_memory_lock:
                        snapshot = dict(shared_memory)
                    await websocket.send(json.dumps({"status": "ok", "action": "memory", "cmd": "dump", "memory": snapshot}))
                else:
                    await _send_error(websocket, f"unknown memory cmd '{cmd}'")
                continue

            await _send_error(websocket, f"unknown action '{action}'")

    except Exception as exc:
        print(f"[!] Error with {client_name or remote}: {exc}")
    finally:
        if client_name:
            with clients_lock:
                clients.pop(client_name, None)
            print(f"[-] Disconnected: {client_name}  ({remote})")
        else:
            print(f"[-] Disconnected unregistered: {remote}")


async def run_server(host="localhost", port=8766):
    """Start the relay server.

    Parameters
    ----------
    host
        Bind address.
    port
        Bind port.
    """
    async with serve(relay_handler, host, port):
        print(f"Relay server listening on ws://{host}:{port}")
        await asyncio.get_running_loop().create_future()


def main():
    """CLI entry point for ``transfer-server``."""
    import argparse

    parser = argparse.ArgumentParser(description="Transfer relay server")
    parser.add_argument("--host", default="localhost", help="Bind address")
    parser.add_argument("--port", type=int, default=8766, help="Bind port")
    args = parser.parse_args()
    asyncio.run(run_server(args.host, args.port))


if __name__ == "__main__":
    main()
