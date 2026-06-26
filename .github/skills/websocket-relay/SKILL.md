---
name: websocket-relay
description: 'WebSocket relay broker and client for RPC-style message passing between clients with shared memory. Use when: setting up message relay, running the relay broker, connecting clients, testing inter-client communication, remote procedure calls, working with shared state between WebSocket clients, monitoring and observability.'
---

# WebSocket Relay

An async WebSocket relay broker that routes binary-framed messages between named clients. Each client exposes a dispatch table of functions that other clients can call remotely.

## Components

| File | Purpose |
|---|---|
| `cli.py` | Unified `nexus-transfers` entry point — dispatches to subcommands |
| `protocol.py` | `encode_frame` / `decode_frame` — the binary wire format |
| `broker.py` | Relay broker on `ws://localhost:8766`. Routes frames to named targets; only decodes JSON when the frame is addressed to the broker itself. Manages monitor registrations and broadcasts events. |
| `client.py` | RPC client class + interactive CLI. Registers, dispatches incoming calls, sends binary file chunks, emits monitoring events. |
| `monitor.py` | CLI tool (`nexus-transfers monitor`) that registers as a monitoring service and prints broadcast events. |
| `copy.py` | Recursive remote-to-local directory copy via relay/S3. |
| `copy_ssh.py` | Local-to-remote SSH/SFTP directory copy. |
| `kill.py` | CLI tool (`nexus-transfers kill`) — kill clients by name/wildcard or `--all`, soft (`-1`) / hard (`-9`). |
| `dispatch.py` | Default dispatch table (`adder`, `echo`, `get_file`, `list_dir`). |

## Wire Protocol (v1)

Every WebSocket message is a **binary frame** with the following layout:

```
 byte  field
 ----  -----
  1    version       uint8, always 1
  1    src_len       uint8
  N    source        sender name (UTF-8); empty = broker
  1    msg_len       uint8
  M    msg_name      message type (UTF-8)
  1    tgt_len       uint8; 0 = frame is for the broker
  K    target        recipient name (UTF-8); absent when tgt_len == 0
  1    encoding      'J' (JSON) or 'R' (raw bytes)
  4    size          payload byte count, big-endian uint32
  P    payload       payload bytes
```

The broker forwards frames with a non-empty target verbatim without decoding the payload. It only decodes JSON when `tgt_len == 0` (broker-addressed frames).

### Message names

| msg_name           | Direction              | Description                                      |
|--------------------|------------------------|--------------------------------------------------|
| `register`         | client → broker        | Register; source field is the client name        |
| `register`         | broker → client        | Registration ack                                 |
| `register_monitor` | client → broker        | Register as a monitoring service                 |
| `register_monitor` | broker → client        | Monitor registration ack                         |
| `monitor_event`    | client → broker        | Emit a monitoring event (broker broadcasts to all monitors) |
| `monitor_event`    | broker → monitor       | Broadcast event to registered monitors           |
| `call`             | client → client (relay)| RPC invocation; JSON payload has `msg_id`, `func`, `args` |
| `reply`            | client → client (relay)| RPC result/error; JSON payload has `msg_id`, `result` or `error` |
| `chunk`            | client → client (relay)| Binary file chunk; R payload = `[2b hdr_len][json hdr][raw data]` |
| `list_clients`     | client → broker        | Request connected client names                   |
| `list_clients`     | broker → client        | Response; JSON payload has `clients` array       |
| `kill`             | client → client (relay)| Ask the target to shut down. JSON payload has `msg_id`, `reason`, `from`, `signal` (`9` = hard `os._exit`, `1` = soft clean exit) |
| `kill_ack`         | client → client (relay)| Acknowledgement sent back just before the target goes down |
| `error`            | broker → client        | Error; JSON payload has `error` (and optionally `msg_id`) |

### Chunk payload format (encoding = R)

```
[2 bytes: json_header_len][json_header bytes][raw chunk bytes]
```

`json_header` fields: `msg_id`, `chunk` (0-based index), `total_chunks`, and `checksum` (SHA-256 hex, last chunk only).

## Monitoring

Multiple clients can register as monitoring services. Events are broadcast to all registered monitors. No replies are given to monitoring messages.

### Registering as a monitor

```python
async with Client("my-monitor", url=broker) as client:
    await client.register_monitor(callback=on_event)
    # client now receives all broadcast events via on_event(dict)
```

### Event format

All dates are ISO 8601 UTC. Events have the following structure:

```json
{
    "type": "...",
    "date": "2025-01-15T10:30:00.123+00:00",
    "source": "client-name",
    "message": "Some descriptive text",
    "task": {
        "name": "...",
        "uuid": "..."
    },
    "progress": {
        "label": "...",
        "uuid": "...",
        "start": "...",
        "update": "...",
        "minimum": 0,
        "maximum": 1000,
        "value": 500,
        "unit": "byte",
        "rate": 2.4
    }
}
```

- `task` and `progress` are optional (omitted for simple messages)
- All events with the same `progress.uuid` refer to the same progress bar

### Event types

| Type | Description |
|---|---|
| `connected` | Server-emitted: a new client registered |
| `disconnected` | Server-emitted: a client disconnected |
| `ok` | Client-emitted: successful operation |
| `info` | Client-emitted: informational message |
| `progress` | Client-emitted: data transfer progress |
| `warning` | Client-emitted: recoverable issue (retry, etc.) |
| `error` | Client-emitted: error condition |

### Emitting events from a client

```python
# Simple message
await client.monitor("operation complete", status="ok")

# Structured event with progress
await client.monitor(
    "transferring file.bin",
    event_type="progress",
    task={"name": "copy", "uuid": "task-123"},
    progress={"label": "file.bin", "uuid": "p-1", "value": 50, "maximum": 100, "unit": "byte"},
)
```

### Server-emitted events

The broker automatically broadcasts `connected` and `disconnected` events when clients register/deregister. These events have `source` set to the client name.

## Dispatch Table

Every client exposes these functions by default:

| Function   | Description                  |
|------------|------------------------------|
| `adder`    | Returns `value + 1`          |
| `echo`     | Returns arguments unchanged  |
| `get_file(path, chunk_size=65536)` | Returns file as binary transfer (requires `--allow-path`); chunk size chosen by caller |
| `list_dir` | Returns paginated directory listing (requires `--allow-path`) |

## Running

All commands are available via the unified `nexus-transfers` CLI:

```bash
# Show available commands
nexus-transfers --help

# Broker (runs forever)
nexus-transfers broker [--host HOST] [--port PORT] [--debug]

# Server — a peer awaiting messages (runs forever)
nexus-transfers server --name a [--allow-path /data]

# Server (interactive shell)
nexus-transfers server --name a --interactive

# Monitor — prints broadcast events (runs forever)
nexus-transfers monitor [--broker-url URL] [--json] [--name NAME]

# Copy — recursive remote-to-local directory copy (terminates)
nexus-transfers copy --from <remote> <src> <local> [--chunk-size BYTES] [--max-concurrent N]

# Copy-ssh — local-to-remote SSH/SFTP copy (terminates)
nexus-transfers copy-ssh --source /local/dir --target user@host:/remote/path

# Kill — terminate connected clients by name or wildcard (terminates)
nexus-transfers kill <name>        # exact name
nexus-transfers kill 'copy-*'      # fnmatch wildcards (* and ?); quote to avoid shell glob
nexus-transfers kill --all         # every client
nexus-transfers kill -1 <name>     # soft only (clean exit 0)
nexus-transfers kill -9 <name>     # hard only (immediate os._exit)
```

### Killing clients

`nexus-transfers kill` enumerates targets with `list_clients`, then sends a `kill`
frame to each match concurrently. Self and `monitor-*` clients are skipped
(`--include-monitors` to include the latter). Signals mirror `kill(1)`:

- `-9` **hard**: target acks then `os._exit(1)` immediately, abandoning any in-flight transfer.
- `-1` **soft**: target acks, emits a `warning` monitor event, closes its connection cleanly, and exits 0.
- **default** (neither flag): send `-1`, wait `--grace` seconds (default 2.0), then `-9` any client still connected.

Robustness: a single client process registers **one** name; its in-process
concurrency (async tasks, `max_concurrent`) all dies with that process. For
`copy-ssh --processes N`, each worker process becomes its own process-group
leader (`os.setpgrp`) and registers its pgid on the coordinator's client via
`Client.register_child_pgid`; the kill handler then `killpg`s each worker group
(SIGKILL for hard, SIGTERM for soft) so workers and their `ssh` grandchildren
are never orphaned.

### Interactive shell commands

| Command                        | Description                        |
|--------------------------------|------------------------------------|
| `send <target>.<func> [args]`  | Call a remote function             |
| `clients`                      | List connected clients             |
| `quit`                         | Disconnect                         |

## Key helpers

| Symbol | File | Purpose |
|---|---|---|
| `encode_frame(source, msg_name, target, encoding, payload)` | `protocol.py` | Build a binary frame |
| `decode_frame(raw)` → `(version, source, msg_name, target, encoding, payload)` | `protocol.py` | Parse a binary frame |
| `Client._emit_event(event)` | `client.py` | Send a structured monitoring event to the broker for broadcast |
| `Client.monitor(message, ...)` | `client.py` | High-level event emitter (fire-and-forget) |
| `Client.register_monitor(callback)` | `client.py` | Register as a monitoring service and set event callback |
| `Client.kill(target, reason="", timeout=5.0, signal=9)` | `client.py` | Tell a peer to shut down (`signal=9` hard, `1` soft); awaits its `kill_ack` |
| `Client.register_child_pgid(pgid)` / `unregister_child_pgid(pgid)` | `client.py` | Track child worker process groups to `killpg` when this client is killed |
| `Client._dispatch_call(sender, msg_id, payload)` | `client.py` | Shared RPC dispatch used by both listener modes |
| `Client._send_file_chunks(target, msg_id, ft)` | `client.py` | Send a `FileTransfer` as R-encoded chunk frames |
| `Client._receive_chunk_payload(raw_payload)` | `client.py` | Buffer incoming chunk payloads; returns assembled bytes when complete |
| `_broadcast_event(event)` | `broker.py` | Send event to all registered monitors (fire-and-forget) |
