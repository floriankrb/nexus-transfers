# nexus-transfers

WebSocket relay broker with named RPC routing, binary file transfer, and recursive directory sync.

## Installation

Requires Python ≥ 3.12.

```bash
pip install -e .
```

## Quick start

### 1. Start the broker

```bash
nexus-transfers broker --port 8766
```

### 2. Start a client

```bash
nexus-transfers server --name a --broker-url ws://localhost:8766 --allow-path /path/to/share
```

`--allow-path` can be repeated to expose multiple directories for `get_file` and `list_dir` operations. Without it, only built-in RPC functions (`adder`, `echo`) are available.

### 3. Call remote functions

From another terminal (or from Python):

```bash
nexus-transfers server --name b --broker-url ws://localhost:8766
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

## Direct SSH copy (no relay required)

`nexus-transfers copy-ssh` copies a local directory straight to a remote host over
SFTP.  No `nexus-transfers server` on the remote side, no S3, no relay for data — the
relay is used only to send progress messages to a monitor peer.

```
Local filesystem ──► nexus-transfers copy-ssh ──► SFTP ──► SSH target
                                   │
                                   └──► relay ──► monitor (progress only)
```

```bash
nexus-transfers copy-ssh \
    --source /data/dataset.zarr \
    --target user@host:/remote/path \
    --broker-url wss://relay.example.com \
    --max-concurrent 8 \
    --ssh-connections 2
```

Interrupted transfers resume automatically: a file is skipped when its remote
size already matches the local size.

## Features

- **Named routing** — clients register with a unique name; messages are routed by name
- **RPC dispatch** — clients expose functions that other clients can call remotely
- **Binary file transfer** — files are sent as raw binary WebSocket frames (no base64), chunked with tqdm progress bars
- **Optional S3 staging** — pass `use_s3=True` (or `--use-s3` to `nexus-transfers copy`) to relay through an S3-compatible bucket instead of the WebSocket
- **SHA-256 checksums** — computed incrementally during transfer and verified on completion
- **Recursive directory sync** — `get_directory` walks the remote tree and downloads files in parallel (configurable concurrency), resuming interrupted transfers by comparing file sizes
- **Direct SSH copy** — `nexus-transfers copy-ssh` uploads a local directory via SFTP without any relay involvement in the data path
- **Path security** — `get_file` and `list_dir` validate paths against an allow-list using `realpath`; `..` traversal is rejected
- **Shared memory** — key-value store on the broker, accessible from any client via `/mem` commands
- **Client discovery** — `list_clients` / `/clients` returns all connected client names

## S3 staging

Configure on both clients:

```bash
export NEXUS_TRANSFER_S3_BUCKET=my-bucket
export NEXUS_TRANSFER_S3_ENDPOINT_URL=https://s3.example.com   # optional
export NEXUS_TRANSFER_S3_ACCESS_KEY_ID=...                     # optional
export NEXUS_TRANSFER_S3_SECRET_ACCESS_KEY=...                 # optional
```

Then:

```bash
nexus-transfers copy --from a /remote/path ./local-path --use-s3
```

Flow: provider uploads → returns key/size/sha256 → initiator downloads
from S3 → initiator tells provider to delete the staged object. The file
on the provider's disk is untouched.

## CLI reference

### `nexus-transfers broker`

| Flag     | Default     | Description   |
|----------|-------------|---------------|
| `--host` | `localhost` | Bind address  |
| `--port` | `8766`      | Bind port     |

### `nexus-transfers server`

| Flag            | Default                  | Description                                      |
|-----------------|--------------------------|--------------------------------------------------|
| `--name`        | (required)               | Unique client ID                                 |
| `--broker-url`  | `ws://localhost:8766`    | Broker WebSocket URL                             |
| `--allow-path`  | (none)                   | Directory to expose for file operations (repeatable) |

### `nexus-transfers copy`

| Flag               | Default               | Description                                            |
|--------------------|-----------------------|--------------------------------------------------------|
| `--from`           | (required)            | Name of the remote client                              |
| `source target`    | (required)            | Remote source dir, local target dir                    |
| `--broker-url`     | `ws://localhost:8766` | Broker WebSocket URL                                   |
| `--max-concurrent` | `4`                   | Maximum parallel file transfers                        |
| `--chunk-size`     | `65536`               | Binary chunk size (ignored with `--use-s3`)            |
| `--use-s3`         | off                   | Stage transfers through S3 (needs `NEXUS_TRANSFER_S3_*`) |

### `nexus-transfers copy-ssh`

| Flag               | Default                | Description                                         |
|--------------------|------------------------|-----------------------------------------------------|
| `--source`         | (required)             | Local directory to copy                             |
| `--target`         | (required)             | `[user@]host:/remote/path`                          |
| `--broker-url`     | `$NEXUS_TRANSFERS_URL` | Relay URL for monitoring only (optional)            |
| `--name`           | auto-generated         | Client name on the relay                            |
| `--site`           | (none)                 | Site label for monitor messages                     |
| `--max-concurrent` | `4`                    | Number of parallel SFTP uploads                     |
| `--ssh-port`       | `22`                   | SSH port on the target host                         |
| `--ssh-key`        | SSH agent / default    | Path to private key file                            |
| `--ssh-connections`| `2`                    | Number of SSH connections to open (see note below)  |
| `--size`           | off                    | Show byte-based progress instead of file count      |
| `--no-verify`      | off                    | Skip TLS verification for the relay connection      |
| `--debug`          | off                    | Enable debug logging                                |

**`--ssh-connections` vs `--max-concurrent`**: `--ssh-connections` controls how
many TCP connections are opened to the SSH server.  Each connection carries one
SFTP session, and the `--max-concurrent` upload workers are distributed across
those sessions in round-robin order.  Opening more than one connection lets
multiple SFTP sessions run in parallel, which can saturate bandwidth that a
single SSH connection cannot fully use (SSH multiplexes all channels over one
TCP stream, so a single connection is limited by its flow-control window).  Two
connections is a reasonable default; raise it if the link is fast and latency is
high.
