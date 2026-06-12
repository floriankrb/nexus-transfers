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
nexus-transfers server --name b --broker-url ws://localhost:8766 --interactive
```

Then in the interactive prompt (without `--interactive` the client runs as a
headless RPC worker):

```
send a.echo hello
send a.adder 42
send a.list_dir "."
clients
quit
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

        # Transfer a single file over the relay (returns bytes).
        # Without use_s3=False the transfer is staged through S3, which
        # requires NEXUS_TRANSFER_S3_BUCKET to be set on the remote side.
        data = await client.send("a.get_file", "data.bin", use_s3=False)

        # Recursively copy a remote directory (resumes interrupted
        # transfers).  Staged through S3 by default; pass use_s3=False to
        # send the data over the relay instead.
        await client.get_directory("a", "src", "./local-copy")

asyncio.run(main())
```

### Serving files

Pass `allowed_paths` to expose directories for `get_file` and `list_dir`:

```python
async with Client("worker", allowed_paths=["/data", "/models"]) as client:
    await asyncio.Future()  # keep running
```

### Calling `copy` and `copy-ssh` from Python

Both CLI commands have importable async counterparts:

```python
from nexus_transfers.copy import copy
from nexus_transfers.copy_ssh import _copy_to_ssh

# Equivalent to: nexus-transfers copy --from a /remote/src ./local-copy
asyncio.run(copy(
    name="my-copy",
    broker_url="ws://localhost:8766",
    remote_client="a",
    source="/remote/src",
    target="./local-copy",
    max_concurrent=4,
    use_s3=True,
    track_bytes=False,
))

# Equivalent to: nexus-transfers copy-ssh --source /data --target user@host:/remote
asyncio.run(_copy_to_ssh(
    source="/data",
    target="user@host:/remote",
    broker_url="ws://localhost:8766",  # None to skip monitoring
    name="my-ssh-copy",
    site=None,
    max_concurrent=4,
    ssh_port=22,
    ssh_key=None,
    ssh_connections=2,
    track_bytes=False,
    ssl_verify=True,
))
```

### Progress callbacks

The relay broadcasts a progress event roughly every 30 seconds during a copy.
Register a monitoring client with `on_monitor_event` to receive these events:

```python
def on_progress(event: dict) -> None:
    # event keys: type, message, source, date
    # type is "progress", "ok", "error", or "info"
    print(event["source"], event["message"])

async with Client("monitor", url="ws://localhost:8766",
                  on_monitor_event=on_progress) as monitor:
    await monitor.register_monitor()
    await asyncio.Future()  # keep receiving events
```

You can also set the handler after construction:

```python
client.on_monitor_event = my_callback
```

Or pass it to `register_monitor`:

```python
await client.register_monitor(callback=my_callback)
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

## Integrity check

`nexus-transfers check-files` verifies a local copy against a remote nexus
reference (hashes are computed on each side; no file content is transferred),
and `nexus-transfers check-files-ssh` verifies a remote SSH copy against the
local reference. Both detect corruption, missing files, extra files and
permission drift, exit non-zero on unfixed discrepancies, and can repair with
`--fix`, `--delete-extra` and `--fix-permissions MODE` (explicit octal mode,
e.g. `600`). See [CHECK_FILES.md](CHECK_FILES.md).

```bash
nexus-transfers check-files --from a /remote/path ./local-path --fix
nexus-transfers check-files-ssh --source /data --target user@host:/remote --fix
```

## Features

- **Named routing** — clients register with a unique name; messages are routed by name
- **RPC dispatch** — clients expose functions that other clients can call remotely
- **Binary file transfer** — files are sent as raw binary WebSocket frames (no base64), chunked with rich progress bars
- **S3 staging (default)** — transfers are staged through an S3-compatible bucket; pass `use_s3=False` (or `--use-broker` to `nexus-transfers copy`) to send the data over the WebSocket relay instead
- **SHA-256 checksums** — computed incrementally during transfer and verified on completion
- **Recursive directory sync** — `get_directory` walks the remote tree and downloads files in parallel (configurable concurrency), resuming interrupted transfers by comparing file sizes
- **Direct SSH copy** — `nexus-transfers copy-ssh` uploads a local directory via SFTP without any relay involvement in the data path
- **Path security** — `get_file` and `list_dir` validate paths against an allow-list using `realpath`; `..` traversal is rejected
- **Client discovery** — `list_clients` (or `clients` in the interactive prompt) returns all connected client names

## S3 staging

S3 staging is the **default** transfer mode. Configure on the providing
client (the receiving side learns the bucket name from the provider's
reply):

```bash
export NEXUS_TRANSFER_S3_BUCKET=my-bucket
export NEXUS_TRANSFER_S3_ENDPOINT_URL=https://s3.example.com   # optional
export NEXUS_TRANSFER_S3_ACCESS_KEY_ID=...                     # optional
export NEXUS_TRANSFER_S3_SECRET_ACCESS_KEY=...                 # optional
```

Then:

```bash
nexus-transfers copy --from a /remote/path ./local-path
```

To bypass S3 and send the data over the WebSocket relay instead:

```bash
nexus-transfers copy --from a /remote/path ./local-path --use-broker
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
| `--interactive` | off                      | Start an interactive prompt instead of a headless worker |

### `nexus-transfers copy`

| Flag               | Default               | Description                                            |
|--------------------|-----------------------|--------------------------------------------------------|
| `--from`           | (required)            | Name of the remote client                              |
| `source target`    | (required)            | Remote source dir, local target dir                    |
| `--broker-url`     | `ws://localhost:8766` | Broker WebSocket URL                                   |
| `--max-concurrent` | `4`                   | Maximum parallel file transfers                        |
| `--chunk-size`     | `65536`               | Binary chunk size (only used with `--use-broker`)      |
| `--use-broker`     | off                   | Send data over the relay instead of S3 staging (S3 is the default and needs `NEXUS_TRANSFER_S3_*`) |

### `nexus-transfers copy-ssh`

| Flag               | Default                | Description                                         |
|--------------------|------------------------|-----------------------------------------------------|
| `--source`         | (required)             | Local directory to copy                             |
| `--target`         | (required)             | `[user@]host:/remote/path`                          |
| `--broker-url`     | (none — monitoring disabled) | Relay URL for monitoring only (optional)      |
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
