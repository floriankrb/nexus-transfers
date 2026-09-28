# nexus-transfers

WebSocket relay broker with named RPC routing, binary file transfer, and recursive directory sync.

A single script, `nexus-transfers <command>`. The broker routes frames
between named peers; peers expose RPCs (`list_dir`, `get_file`,
`hash_file`, …) and move bytes over the relay, through S3 staging, or
directly over SSH / S3.

Further reading:

| Doc | Content |
|-----|---------|
| [docs/USAGE.md](docs/USAGE.md) | Guide: SSH and S3 copy, S3 staging, integrity checks, locks, kill, monitoring |
| [docs/CLI.md](docs/CLI.md) | Every command and flag |
| [docs/CONFIGURATION.md](docs/CONFIGURATION.md) | Precedence, environment variables, `[nexus]` settings tables, broker URL resolution |
| [docs/PYTHON_API.md](docs/PYTHON_API.md) | `Client`, the transfer API (`nexus_transfers.api`), progress callbacks |
| [docs/PROTOCOL.md](docs/PROTOCOL.md) | Frame layout, messages, pending calls, name locks, RPCs, checksums, monitor events |
| [docs/BACKENDS.md](docs/BACKENDS.md) | Relay, S3 staging, SSH push, direct S3: data paths, resume, atomic writes |
| [docs/CHECK_FILES.md](docs/CHECK_FILES.md) | `check-files`, `check-files-ssh`, `check-files-s3` |
| [docs/ROADMAP.md](docs/ROADMAP.md) | Open items |
| [docs/prompts/](docs/prompts/README.md) | The design prompts (history) |

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

`--allow-path` can be repeated to expose multiple directories for `get_file`, `list_dir` and `hash_file` operations. Without it, only built-in RPC functions (`adder`, `echo`) are available.

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

### 4. Copy and watch

```bash
nexus-transfers monitor --broker-url ws://localhost:8766          # terminal 1
nexus-transfers copy --from a /path/to/share/dir ./local-copy --use-broker
```

## Commands

| Command | Runs | Purpose |
|---------|------|---------|
| `broker` | forever | WebSocket relay broker |
| `monitor` | forever | Register as monitor, print broadcast events |
| `server` | forever | Run a peer serving RPCs (headless, or `--interactive`) |
| `copy` | once | Copy a remote peer's directory to local (S3 staging, or relay with `--use-broker`) |
| `copy-ssh` | once | Push a local file/directory to `[user@]host:/path` over SFTP |
| `copy-to-s3` | once | Local file/directory → `s3://bucket[/prefix]` |
| `copy-from-s3` | once | `s3://bucket/key-or-prefix` → local |
| `check` | once | Diagnose configuration: S3 credentials round-trip (`--s3`), site broker (`--site`) |
| `check-files` | once | Verify a local copy against a remote peer's reference |
| `check-files-ssh` | once | Verify a remote SSH copy against the local reference |
| `check-files-s3` | once | Verify an S3 copy against the local reference |
| `kill` | once | Kill connected clients by name / wildcard, or `--all` |

`nexus-transfers <command> --help` lists every flag; the full reference is
[docs/CLI.md](docs/CLI.md), worked examples are in
[docs/USAGE.md](docs/USAGE.md), settings in
[docs/CONFIGURATION.md](docs/CONFIGURATION.md) and the Python API in
[docs/PYTHON_API.md](docs/PYTHON_API.md).

## Features

- **Named routing** — clients register with a unique name; messages are routed by name
- **RPC dispatch** — clients expose functions that other clients can call remotely
- **Binary file transfer** — files are sent as raw binary WebSocket frames (no base64), chunked with rich progress bars
- **S3 staging (default)** — transfers are staged through an S3-compatible bucket; pass `use_s3=False` (or `--use-broker` to `nexus-transfers copy`) to send the data over the WebSocket relay instead
- **SHA-256 checksums** — computed incrementally during transfer and verified on completion
- **Recursive directory sync** — `get_directory` walks the remote tree and downloads files in parallel (configurable concurrency), resuming interrupted transfers
- **Atomic writes** — every download / upload goes to `<name>.<8hex>.tmp` and is renamed into place
- **Direct SSH copy** — `nexus-transfers copy-ssh` uploads a local directory via SFTP without any relay involvement in the data path
- **Direct S3 copy** — `nexus-transfers copy-to-s3` / `copy-from-s3` move a local file or directory to/from an S3 bucket without any peer or relay
- **Integrity check** — `check-files*` compare hashes / sizes against a reference and repair
- **Dead-peer detection** — the broker errors pending calls when the callee disconnects
- **Name locks** — unique names double as a distributed mutex (`--steal`, `api.push_ssh(lock=…)`)
- **Path security** — `get_file`, `list_dir` and `hash_file` validate paths against an allow-list using `realpath`; `..` traversal is rejected
- **Client discovery** — `list_clients` (or `clients` in the interactive prompt) returns all connected client names

## Deployment

`etc/nexus-transfers.service` is a sample systemd user unit for the broker.
Copy it to `~/.config/systemd/user/` and point `ExecStart` at your own
checkout — systemd does not search `PATH`, so the venv's absolute path is
required:

```ini
ExecStart=/path/to/nexus-transfers/.venv/bin/nexus-transfers broker --host 127.0.0.1 --port 8766
```

Put a TLS-terminating proxy in front and give clients a `wss://` URL when
using Basic Auth.
