---
name: websocket-relay
description: 'WebSocket relay server and client for RPC-style message passing between clients with shared memory. Use when: setting up message relay, running the relay server, connecting clients, testing inter-client communication, remote procedure calls, working with shared state between WebSocket clients.'
---

# WebSocket Relay

A multithreaded WebSocket relay server that routes JSON messages between named clients. Each client exposes a dispatch table of functions that other clients can call remotely. Both connections share the same in-process memory store (thread-safe dict).

## Components

- **server.py** – Async WebSocket server on `ws://localhost:8766`. Routes JSON messages to named targets and supports shared-memory commands.
- **client.py** – Interactive CLI client with a built-in dispatch table. Registers with `--name`, exposes functions (`adder`, `echo`), auto-dispatches incoming calls and replies with results or exception details.

## Dispatch Table

Every client exposes these functions:

| Function | Description              | Example call         |
|----------|--------------------------|----------------------|
| `adder`  | Returns `value + 1`      | `/send b.adder 42`   |
| `echo`   | Returns arguments as-is  | `/send b.echo "hello"`|

If a function raises an exception, the caller receives the error message and full traceback. The target client does not terminate.

## Protocol

All communication uses JSON. Every message has an `"action"` field:

| Action     | Direction        | Description                            |
|------------|------------------|----------------------------------------|
| `register` | client → server  | Register with a unique name            |
| `send`     | client → server  | RPC call to `target.func`              |
| `message`  | server → target  | Deliver a call from another client     |
| `reply`    | client → server  | Auto-reply with result or error        |
| `reply`    | server → sender  | Deliver the result back to the caller  |
| `memory`   | client ↔ server  | Shared-memory operations (set/get/dump)|

## Running

Start the server:

```bash
~/work/transfers/.venv/bin/python3 ~/work/transfers/server.py
```

Start two clients:

```bash
~/work/transfers/.venv/bin/python3 ~/work/transfers/client.py --name a
~/work/transfers/.venv/bin/python3 ~/work/transfers/client.py --name b
```

From client `a`, call a function on client `b`:

```
/send b.adder 42
# result: 43

/send b.echo "hello world"
# result: "hello world"
```

## Client Commands

| Command                          | Description                              |
|----------------------------------|------------------------------------------|
| `/send <target>.<func> <args>`   | Call a function on a remote client       |
| `/mem set <key> <value>`         | Store a value in shared memory           |
| `/mem get <key>`                 | Retrieve a value from shared memory      |
| `/mem dump`                      | Show all shared memory contents          |
| `/quit`                          | Disconnect                               |

## Dependencies

- Python 3.12+
- `websockets` (installed in `.venv`)

## Venv

Always use the venv at `~/work/transfers/.venv/bin/python3`.
