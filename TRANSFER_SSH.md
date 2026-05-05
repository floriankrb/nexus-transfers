# nexus-copy-to-ssh

Direct local-to-SSH file transfer with relay-based monitoring.

## Overview

`nexus-copy-to-ssh` runs on the machine holding the source files.  It
walks a local directory, opens a pool of SFTP connections to a remote
SSH target, and copies files concurrently.  It connects to the relay
server **only** to send progress messages to the monitor peer.

```
Source filesystem ──► nexus-copy-to-ssh ──► SFTP ──► SSH target
                            │
                            └──► relay ──► monitor (progress only)
```

No `nexus-client` is needed.  No S3.  No RPC dispatch.  Data moves
once, directly from source to destination.

## CLI interface

```
nexus-copy-to-ssh \
    --source /data/dataset.zarr \
    --target user@host:/remote/path \
    --server-url wss://relay.example.com \
    --name copy-meluxina-abc123 \
    --site meluxina \
    --max-concurrent 8 \
    --ssh-port 22 \
    --ssh-key ~/.ssh/id_ed25519 \
    --size \
    --debug
```

### Arguments

| Flag               | Default                  | Description                                         |
|--------------------|--------------------------|-----------------------------------------------------|
| `--source`         | (required)               | Local directory to copy                             |
| `--target`         | (required)               | `user@host:/remote/path` or `host:/remote/path`     |
| `--server-url`     | `$NEXUS_TRANSFERS_URL`   | Relay server URL (for monitoring only)              |
| `--name`           | auto-generated           | Client name on the relay                            |
| `--site`           | `None`                   | Site label for monitor messages                     |
| `--max-concurrent` | `4`                      | Number of parallel SFTP uploads                     |
| `--ssh-port`       | `22`                     | SSH port on target                                  |
| `--ssh-key`        | SSH agent / default key  | Path to private key                                 |
| `--ssh-connections`| `2`                      | Number of SSH connections to open                    |
| `--size`           | off                      | Show byte-based progress instead of file count      |
| `--no-verify`      | off                      | Skip TLS verification for relay connection          |
| `--debug`          | off                      | Enable debug logging                                |

## Target parsing

Parse `user@host:/path` into `(user, host, path)`:

```
user@host:/remote/path  →  user="user",  host="host",  path="/remote/path"
host:/remote/path       →  user=None (current user), host="host", path="/remote/path"
```

Reject anything without a `:`.

## Implementation plan

### 1. New file: `src/nexus_transfers/ssh.py`

Thin wrapper around `asyncssh`.

- `SSHPool` class:
  - `__init__(host, port, user, key_path, num_connections)` — stores
    config, no connections yet.
  - `async connect()` — opens `num_connections` SSH connections.  Each
    connection opens one SFTP client.  Store in a list.
  - `get_sftp()` — returns an SFTP client from the pool (round-robin or
    least-loaded).
  - `async close()` — closes all connections.
  - `async __aenter__` / `__aexit__` — context manager.

- `async write_file(sftp, local_path, remote_path)`:
  - `sftp.makedirs(remote_dir, exist_ok=True)`
  - `sftp.put(local_path, remote_path)` — asyncssh handles streaming.

- `async stat_remote(sftp, remote_path)` → `int | None`:
  - Return file size or `None` if file does not exist.  For resume
    checks.

### 2. New file: `src/nexus_transfers/copy_ssh.py`

Main logic and CLI entry point.

#### Walk

Reuse the same pattern as `get_directory` but locally:

- `_walk_local(source_path, queue)` — walks using `os.scandir`,
  pushes `(local_file, relative_path, size)` tuples onto an
  `asyncio.Queue`.  Runs in an executor so NFS stat calls don't block
  the event loop.

#### Workers

`max_concurrent` worker coroutines, each:

1. Pull `(local_file, rel_path, size)` from queue.
2. Compute `remote_path = target_base / rel_path`.
3. Resume check: `stat_remote(sftp, remote_path)` — skip if sizes
   match.
4. `write_file(sftp, local_file, remote_path)`.
5. Update progress bar.
6. Every 30 s, send progress to monitor.

#### Monitor connection

- Create a `Client` instance with an empty dispatch table, connect to
  the relay.  Use only `client.monitor()` to send progress.
- If the relay is unreachable, continue without monitoring (log a
  warning).
- Close the client on exit.

#### Progress bar

Reuse the existing `_CountOrBytesColumn` and `_BinarySpeedColumn` from
`client.py`.  Extract them to a shared module or import directly.

#### Entry point

```python
async def _copy_to_ssh(source, target, server_url, name, site,
                        max_concurrent, ssh_port, ssh_key,
                        ssh_connections, track_bytes, **client_kwargs):
    pool = SSHPool(host, port, user, key_path, ssh_connections)
    async with pool:
        client = Client(name, server_url, dispatch={}, **client_kwargs)
        try:
            await client.connect()
        except Exception:
            _logger.warning("Relay unavailable, continuing without monitor")
            client = None

        # walk + workers + progress (same gather pattern as get_directory)

        if client:
            await client.monitor(f"{name}: {summary}", status="ok")
            await client.close()
```

### 3. Entry point in `pyproject.toml`

```toml
[project.scripts]
nexus-copy-to-ssh = "nexus_transfers.copy_ssh:main"
```

### 4. Dependency

Add `asyncssh` to `[project.dependencies]` in `pyproject.toml`.  Not
optional — required.

### 5. Tests

- `test_copy_ssh.py`:
  - Test target parsing (`user@host:/path`).
  - Test local walk produces correct queue entries.
  - Integration test using `asyncssh` with a local SFTP server
    (asyncssh can serve as well) — or mock the SFTP client.
  - Test resume logic (skip file when remote size matches).

## Shared code to extract

The following currently live in `client.py` but are useful for
`copy_ssh.py`:

- `_CountOrBytesColumn`, `_BinarySpeedColumn`, `_fmt_binary` — progress
  bar helpers.
- `_write_file` — not needed for SSH path but keep for reference.

Consider moving progress bar helpers to a `_progress.py` module so both
`client.py` and `copy_ssh.py` can import them.

## Summary

| Component           | File                   | Lines (est.) |
|---------------------|------------------------|--------------|
| SSH pool + helpers  | `ssh.py`               | ~80          |
| CLI + copy logic    | `copy_ssh.py`          | ~200         |
| Progress extraction | `_progress.py`         | ~40          |
| Tests               | `test_copy_ssh.py`     | ~100         |
| pyproject.toml      | entry point + dep      | ~3           |
| **Total**           |                        | **~420**     |
