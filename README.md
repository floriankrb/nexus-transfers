# nexus-transfers

WebSocket relay server with named RPC routing, binary file transfer, and recursive directory sync.

## Installation

Requires Python ≥ 3.12.

```bash
pip install -e .
```

## Quick start

### 1. Start the server

```bash
nexus-server --port 8766
```

### 2. Start a client

```bash
nexus-client --name a --server-port 8766 --allow-path /path/to/share
```

`--allow-path` can be repeated to expose multiple directories for `get_file` and `list_dir` operations. Without it, only built-in RPC functions (`adder`, `echo`) are available.

### 3. Call remote functions

From another terminal (or from Python):

```bash
nexus-client --name b --server-port 8766
```

Then in the interactive prompt:

```
/send a.echo hello
/send a.adder 42
/send a.list_dir .
/clients
/quit
```

## Python API

```python
import asyncio
from nexus_transfers import Client

async def main():
    async with Client("my-client", url="ws://localhost:8766") as client:
        # List connected clients
        clients = await client.list_clients()

        # Call a function on client "a"
        result = await client.send("a.adder", 42)

        # List a remote directory
        entries = await client.send("a.list_dir", ".")

        # Transfer a single file (returns bytes)
        data = await client.send("a.get_file", "data.bin")

        # Recursively copy a remote directory (resumes interrupted transfers)
        await client.get_directory("a", "src", "./local-copy")

asyncio.run(main())
```

### Serving files

Pass `allowed_paths` to expose directories for `get_file` and `list_dir`:

```python
async with Client("worker", allowed_paths=["/data", "/models"]) as client:
    await asyncio.Future()  # keep running
```

## Features

- **Named routing** — clients register with a unique name; messages are routed by name
- **RPC dispatch** — clients expose functions that other clients can call remotely
- **Binary file transfer** — files are sent as raw binary WebSocket frames (no base64), chunked with tqdm progress bars
- **SHA-256 checksums** — computed incrementally during transfer and verified on completion
- **Recursive directory sync** — `get_directory` walks the remote tree and downloads files in parallel (configurable concurrency), resuming interrupted transfers by comparing file sizes
- **Path security** — `get_file` and `list_dir` validate paths against an allow-list using `realpath`; `..` traversal is rejected
- **Shared memory** — key-value store on the server, accessible from any client via `/mem` commands
- **Client discovery** — `list_clients` / `/clients` returns all connected client names

## CLI reference

### `nexus-server`

| Flag     | Default     | Description   |
|----------|-------------|---------------|
| `--host` | `localhost` | Bind address  |
| `--port` | `8766`      | Bind port     |

### `nexus-client`

| Flag            | Default                  | Description                                      |
|-----------------|--------------------------|--------------------------------------------------|
| `--name`        | (required)               | Unique client ID                                 |
| `--server-url`  | `ws://localhost:8766`    | Server WebSocket URL                             |
| `--allow-path`  | (none)                   | Directory to expose for file operations (repeatable) |
