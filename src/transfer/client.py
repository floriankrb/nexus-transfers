"""WebSocket RPC client – importable class and interactive CLI."""

import asyncio
import json
import traceback
import uuid

from websockets.asyncio.client import connect

from transfer.dispatch import DISPATCH


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
    """

    def __init__(self, name, url="ws://localhost:8766", dispatch=None):
        self.name = name
        self.url = url
        self.dispatch = dispatch if dispatch is not None else dict(DISPATCH)
        self._ws = None
        self._pending = {}
        self._listener_task = None

    async def connect(self):
        """Connect to the server and register."""
        self._ws = await connect(self.url).__aenter__()
        await self._ws.send(json.dumps({"action": "register", "name": self.name}))
        resp = json.loads(await self._ws.recv())
        if resp.get("status") != "ok":
            raise RuntimeError(f"Registration failed: {resp.get('error')}")
        self._listener_task = asyncio.create_task(self._listener())

    async def close(self):
        """Disconnect from the server."""
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        if self._ws:
            await self._ws.close()

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *exc):
        await self.close()

    async def send(self, target_func, *args):
        """Call a function on a remote client and return the result.

        Parameters
        ----------
        target_func
            ``"<target>.<func>"`` string, e.g. ``"a.adder"``.
        *args
            Arguments to pass to the remote function.

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

        envelope = {
            "action": "send",
            "to": target,
            "msg_id": msg_id,
            "payload": {"func": func_name, "args": list(args)},
        }
        await self._ws.send(json.dumps(envelope))
        return await future

    async def _listener(self):
        """Background task that handles incoming messages."""
        try:
            async for raw in self._ws:
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

                    func = self.dispatch.get(func_name)
                    if func is None:
                        result_payload = {
                            "error": f"unknown function '{func_name}'",
                            "traceback": None,
                        }
                    else:
                        try:
                            if isinstance(func_args, list):
                                result = func(*func_args)
                            else:
                                result = func(func_args)
                            result_payload = {"result": result}
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
                    await self._ws.send(json.dumps(reply))
                    continue

                # --- Reply to our earlier call ---
                if action == "reply":
                    msg_id = msg.get("msg_id")
                    payload = msg.get("payload", {})
                    future = self._pending.pop(msg_id, None)
                    if future and not future.done():
                        if "error" in payload:
                            future.set_exception(
                                RemoteError(payload["error"], payload.get("traceback"))
                            )
                        else:
                            future.set_result(payload.get("result"))
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
# Interactive CLI
# ---------------------------------------------------------------------------

async def _interactive(name, url):
    """Run the interactive client loop."""
    async with Client(name, url) as client:
        funcs = ", ".join(client.dispatch.keys())
        print(f"Connected to {url} as '{name}'")
        print(f"Registered functions: {funcs}")
        print("Commands: /send <target>.<func> <args> | /mem set|get|dump ... | /quit\n")

        # Override the listener to also print to the terminal
        client._listener_task.cancel()
        try:
            await client._listener_task
        except asyncio.CancelledError:
            pass
        asyncio.create_task(_interactive_listener(client))

        loop = asyncio.get_event_loop()
        while True:
            try:
                line = await loop.run_in_executor(
                    None, lambda: input(f"[{name}] > ")
                )
            except (EOFError, KeyboardInterrupt):
                break

            line = line.strip()
            if not line:
                continue

            if line == "/quit":
                break

            elif line.startswith("/send "):
                parts = line.split(None, 2)
                if len(parts) < 2:
                    print("  Usage: /send <target>.<func> [args]")
                    continue
                target_func = parts[1]
                if "." not in target_func:
                    print("  Usage: /send <target>.<func> [args]")
                    print("  Example: /send b.adder 42")
                    continue
                target, func_name = target_func.split(".", 1)
                raw_args = parts[2] if len(parts) > 2 else ""

                # Try to parse args as JSON, fall back to string
                try:
                    func_args = json.loads(raw_args)
                except (json.JSONDecodeError, ValueError):
                    func_args = raw_args

                # Wrap non-list args in a list for consistent calling
                if not isinstance(func_args, list):
                    func_args = [func_args] if func_args != "" else []

                msg_id = str(uuid.uuid4())[:8]
                envelope = {
                    "action": "send",
                    "to": target,
                    "msg_id": msg_id,
                    "payload": {"func": func_name, "args": func_args},
                }
                await client._ws.send(json.dumps(envelope))

            elif line.startswith("/mem "):
                parts = line.split(None, 3)
                cmd = parts[1] if len(parts) > 1 else ""
                if cmd == "set" and len(parts) >= 4:
                    await client._ws.send(json.dumps({"action": "memory", "cmd": "set", "key": parts[2], "value": parts[3]}))
                elif cmd == "get" and len(parts) >= 3:
                    await client._ws.send(json.dumps({"action": "memory", "cmd": "get", "key": parts[2]}))
                elif cmd == "dump":
                    await client._ws.send(json.dumps({"action": "memory", "cmd": "dump"}))
                else:
                    print("  Usage: /mem set <key> <val> | /mem get <key> | /mem dump")

            else:
                print("  Unknown command. Use /send, /mem, or /quit")

    print("Disconnected.")


async def _interactive_listener(client):
    """Listener for interactive mode that prints results."""
    name = client.name
    try:
        async for raw in client._ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                print(f"\n  << {raw}")
                print(f"[{name}] > ", end="", flush=True)
                continue

            if not isinstance(msg, dict):
                print(f"\n  << {raw}")
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

                print(f"\n  [call from {sender}] {func_name}({func_args})")

                func = client.dispatch.get(func_name)
                if func is None:
                    result_payload = {
                        "error": f"unknown function '{func_name}'",
                        "traceback": None,
                    }
                else:
                    try:
                        if isinstance(func_args, list):
                            result = func(*func_args)
                        else:
                            result = func(func_args)
                        result_payload = {"result": result}
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
                print(f"  -> replied: {result_payload}")
                print(f"[{name}] > ", end="", flush=True)
                continue

            # --- Reply to our earlier call ---
            if action == "reply":
                payload = msg.get("payload", {})
                sender = msg.get("from", "?")
                msg_id = msg.get("msg_id", "?")
                if "error" in payload:
                    print(f"\n  [error from {sender}] (id={msg_id}) {payload['error']}")
                    if payload.get("traceback"):
                        print(f"  {payload['traceback']}")
                else:
                    print(f"\n  [result from {sender}] (id={msg_id}) {json.dumps(payload.get('result'))}")
                print(f"[{name}] > ", end="", flush=True)
                continue

            # --- Server status messages ---
            if status == "ok" and msg.get("action") == "memory":
                cmd = msg.get("cmd")
                if cmd == "get":
                    print(f"\n  [memory] {msg['key']} = {msg['value']!r}")
                elif cmd == "dump":
                    print(f"\n  [memory] {json.dumps(msg['memory'], indent=2)}")
                elif cmd == "set":
                    print(f"\n  [memory] stored {msg['key']}")
                else:
                    print(f"\n  << {raw}")
                print(f"[{name}] > ", end="", flush=True)
                continue

            if status == "error":
                print(f"\n  [server error] {msg.get('error')}")
                print(f"[{name}] > ", end="", flush=True)
                continue

            print(f"\n  << {raw}")
            print(f"[{name}] > ", end="", flush=True)

    except Exception:
        pass


def main():
    """CLI entry point for ``transfer-client``."""
    import argparse

    parser = argparse.ArgumentParser(description="Transfer RPC client")
    parser.add_argument("--name", required=True, help="Unique client ID")
    parser.add_argument("--url", default="ws://localhost:8766", help="Server URL")
    args = parser.parse_args()
    asyncio.run(_interactive(args.name, args.url))


if __name__ == "__main__":
    main()
