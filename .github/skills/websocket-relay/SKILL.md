---
name: websocket-relay
description: 'WebSocket relay server and client for RPC-style message passing between clients with shared memory. Use when: setting up message relay, running the relay server, connecting clients, testing inter-client communication, remote procedure calls, working with shared state between WebSocket clients.'
---

# WebSocket Relay

An async WebSocket relay server that routes binary-framed messages between named clients. Each client exposes a dispatch table of functions that other clients can call remotely. Both connections share the same in-process memory store (thread-safe dict).

## Components

| File | Purpose |
|---|---|
| `protocol.py` | `encode_frame` / `decode_frame` — the binary wire format |
| `server.py` | Relay server on `ws://localhost:8766`. Routes frames to named targets; only decodes JSON when the frame is addressed to the server itself. |
| `client.py` | RPC client class + interactive CLI. Registers, dispatches incoming calls, sends binary file chunks. |
| `dispatch.py` | Default dispatch table (`adder`, `echo`, `get_file`, `list_dir`). |

## Wire Protocol (v1)

Every WebSocket message is a **binary frame** with the following layout:

```
 byte  field
 ----  -----
  1    version       uint8, always 1
  1    src_len       uint8
  N    source        sender name (UTF-8); empty = server
  1    msg_len       uint8
  M    msg_name      message type (UTF-8)
  1    tgt_len       uint8; 0 = frame is for the server
  K    target        recipient name (UTF-8); absent when tgt_len == 0
  1    encoding      'J' (JSON) or 'R' (raw bytes)
  4    size          payload byte count, big-endian uint32
  P    payload       payload bytes
```

The server forwards frames with a non-empty target verbatim without decoding the payload. It only decodes JSON when `tgt_len == 0` (server-addressed frames).

### Message names

| msg_name      | Direction              | Description                                      |
|---------------|------------------------|--------------------------------------------------|
| `register`    | client → server        | Register; source field is the client name        |
| `register`    | server → client        | Registration ack                                 |
| `call`        | client → client (relay)| RPC invocation; JSON payload has `msg_id`, `func`, `args` |
| `reply`       | client → client (relay)| RPC result/error; JSON payload has `msg_id`, `result` or `error` |
| `chunk`       | client → client (relay)| Binary file chunk; R payload = `[2b hdr_len][json hdr][raw data]` |
| `list_clients`| client → server        | Request connected client names                   |
| `list_clients`| server → client        | Response; JSON payload has `clients` array       |
| `memory`      | client ↔ server        | Shared-memory `set` / `get` / `dump`             |
| `error`       | server → client        | Error; JSON payload has `error` (and optionally `msg_id`) |

### Chunk payload format (encoding = R)

```
[2 bytes: json_header_len][json_header bytes][raw chunk bytes]
```

`json_header` fields: `msg_id`, `chunk` (0-based index), `total_chunks`, and `checksum` (SHA-256 hex, last chunk only).

## Dispatch Table

Every client exposes these functions by default:

| Function   | Description                  |
|------------|------------------------------|
| `adder`    | Returns `value + 1`          |
| `echo`     | Returns arguments unchanged  |
| `get_file(path, chunk_size=65536)` | Returns file as binary transfer (requires `--allow-path`); chunk size chosen by caller |
| `list_dir` | Returns paginated directory listing (requires `--allow-path`) |

## Running

```bash
# Server
nexus-server [--host HOST] [--port PORT] [--debug]

# Client (headless RPC worker)
nexus-client --name a [--allow-path /data]

# Client (interactive shell)
nexus-client --name a --interactive

# Recursive directory copy
nexus-copy --from <remote> <src> <local> [--chunk-size BYTES] [--max-concurrent N]
```

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
| `Client._dispatch_call(sender, msg_id, payload)` | `client.py` | Shared RPC dispatch used by both listener modes |
| `Client._send_file_chunks(target, msg_id, ft)` | `client.py` | Send a `FileTransfer` as R-encoded chunk frames |
| `Client._receive_chunk_payload(raw_payload)` | `client.py` | Buffer incoming chunk payloads; returns assembled bytes when complete |
| `Model.normalised_packages` | `dispatch.py` | `FileTransfer` marker class — returned by `get_file` to trigger chunked transfer |
