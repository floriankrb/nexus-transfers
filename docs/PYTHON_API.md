# Python API

```python
import asyncio
from nexus_transfers import Client

async def main():
    async with Client("my-client", url="ws://localhost:8766") as client:
        # List connected clients
        clients = await client.list_clients()

        # Call a function on client "a"
        result = await client.send("a.adder", 42)

        # List a remote directory (one page: offset=0, limit=1000)
        entries = await client.send("a.list_dir", ".")

        # Transfer a single file over the relay (returns bytes).
        # Without use_s3=False the transfer is staged through S3, which
        # requires NEXUS_TRANSFERS_S3_BUCKET to be set on the remote side.
        data = await client.send("a.get_file", "data.bin", use_s3=False)

        # Recursively copy a remote directory (resumes interrupted
        # transfers).  Staged through S3 by default; pass use_s3=False to
        # send the data over the relay instead.
        await client.get_directory("a", "src", "./local-copy")

asyncio.run(main())
```

`Client(name, url=None, dispatch=None, allowed_paths=None,
reconnect_retries=0, reconnect_delay=2.0, peer_retries=0, peer_delay=2.0,
call_timeout=None, ssl_verify=True, on_monitor_event=None)` — note the
library defaults are **no** retries (the CLIs default to `-1`, forever).
Errors: `RemoteError` (remote exception; `.remote_traceback`),
`PeerNotFoundError` (target not registered / died, retries exhausted),
`NameTakenError` (name held), `ConnectionError`, `asyncio.TimeoutError`
(`call_timeout`). Other methods: `kill(target, reason="", timeout=5.0,
signal=9)`, `monitor(message, status=None, progress=None)`,
`register_monitor(callback=None)`.

## Serving files

Pass `allowed_paths` to expose directories for `get_file`, `list_dir` and `hash_file`:

```python
async with Client("worker", allowed_paths=["/data", "/models"]) as client:
    await asyncio.Future()  # keep running
```

## The transfer API (`nexus_transfers.api`)

The stable, synchronous entry points for a program that drives a transfer
itself (`anemoi-nexus-client` uses them for its ssh mover). This is a public
contract: signatures, the progress-callback shape and the result keys do not
change without coordinating with its users.

```python
from nexus_transfers.api import (
    default_broker_url, delete_ssh, pull_ssh, push_ssh, stat_ssh, verify_ssh,
)

def progress(bytes_done, bytes_total, files_done):
    ...                     # raise here to abort

result = push_ssh(
    "/data/x.zarr", "user@host:/remote/x.zarr",   # a directory or a single file
    progress=progress, progress_interval=5.0,
    lock="nexus-location-<location_uuid>",         # the broker lock (see below)
    broker_url=default_broker_url(),               # None: no lock, no monitoring
)
# {"bytes", "files", "transferred_bytes", "transferred_files",
#  "skipped_bytes", "skipped_files"}
pull_ssh("user@host:/remote/x.zarr", "/data/x.zarr", progress=progress,
         lock="nexus-location-<location_uuid>", broker_url=default_broker_url())
# the same keys; bytes / files are the source's totals
stat_ssh("user@host:/remote/x.zarr")               # {"bytes", "files"} or None
verify_ssh("user@host:/remote/x.zarr", "/data/x.zarr", checksum=False)
verify_ssh("user@host:/remote/x.zarr", {"data/0.0": 100, ".zattrs": 2})
# {"bytes", "files", "missing": [rel…], "mismatched": [rel…], "extra": [rel…]}
delete_ssh("user@host:/store/x.zarr", root="/store")  # {"bytes", "files"} removed
```

The contract, common to every call that takes `progress`:

* `progress(bytes_done, bytes_total, files_done)` is called every
  `progress_interval` seconds and once at the end. **If it raises, the work
  stops** (workers cancelled, connections closed) **and the exception
  propagates** unchanged — how a caller stops work it no longer owns (e.g.
  its Nexus lease was taken over). Work already done stays done; the calls
  are resumable / idempotent, so a re-run finishes it.
* `lock` (`push_ssh`, `pull_ssh`) is a broker client name: the broker keeps
  names unique, so it is a distributed mutex. A stale holder (a hung earlier
  run of the same transfer) is displaced (soft, then hard kill) before the
  work starts; losing the race raises `NameTakenError`. Key it on the
  transfer's identity (the Nexus client uses
  `nexus-location-<location_uuid>`). Without a broker there is no lock.
  Without `lock`, the client name is `push-<8hex>` / `pull-<8hex>`.

The calls:

* `push_ssh(source, target)` — resumable like `copy-ssh` (same-size files
  are skipped; counts in `progress` include them). The other `copy-ssh`
  options (`ssh_port`, `ssh_key`, `ssh_connections`, `processes`,
  `max_concurrent`, `stat_concurrency`, `encryption_algs`, `site`,
  `ssl_verify`, `quiet` (default `True`)) are keyword arguments.
* `pull_ssh(source, target)` — the reverse: an SSH file or directory to a
  local path, same keywords and result (`processes`, `stat_concurrency` and
  `quiet` are accepted and ignored: a pull runs in one process). Local files
  of the remote size are skipped; each file is written as
  `<name>.<8hex>.tmp` and renamed. Nothing at the source →
  `FileNotFoundError`.
* `verify_ssh(target, expected)` — read-only comparison (the
  `check-files-ssh` comparison, nothing fixed or deleted). `expected` is a
  local reference (directory or single file) or a manifest
  `{relative_path: size}`; a single-file target is compared as the
  manifest's one file. Size comparison; `checksum=True` with a local
  reference also compares MD5 of same-size files (`md5sum` on the remote).
  `bytes` / `files` are what the target holds; nothing there → `0` / `0`,
  everything missing. Complete = `missing` and `mismatched` empty. Keywords:
  `checksum`, `progress`, `progress_interval`, `ssh_port`, `ssh_key`,
  `ssh_connections`, `max_concurrent`, `encryption_algs`.
* `delete_ssh(target, root=None)` — removes the file or tree (symlinks
  removed, not followed); nothing there → `{"bytes": 0, "files": 0}`.
  Refuses with `ValueError`, before connecting: an empty, `.`, `..` or
  relative path, fewer than three path components counting `/` (`/`, `/a`),
  and with `root` (the storage root) the root itself or a path outside it.
  Keywords: `root`, `progress`, `progress_interval`, `ssh_port`, `ssh_key`.
* `stat_ssh(target)` — `ssh_port`, `ssh_key`.
* `default_broker_url()` is `$NEXUS_TRANSFERS_URL`, else `[nexus.copy_ssh]
  broker_url` from the config file, else `[nexus] url`, else `None` —
  the same resolution as the `copy-ssh` command's `--broker-url` default.
* Each has an async form: `apush_ssh`, `apull_ssh`, `averify_ssh`,
  `adelete_ssh`, `astat_ssh`.

## Calling `copy` and `copy-ssh` from Python

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

Both accept `on_monitor`, an async callback `on_monitor(message, status=None,
**kwargs)` invoked with every monitor event they emit (no broker
subscription needed); for `status="progress"`, `kwargs["progress"]` holds
`total_transferred`, `files_done`, `files_skipped` and `rate`. Prefer
`nexus_transfers.api` for new code: `_copy_to_ssh` is not a stable name.

## Progress callbacks

Copies emit a progress event roughly every 30 seconds, which the broker
broadcasts to monitors. Register a monitoring client with `on_monitor_event`
to receive these events:

```python
def on_progress(event: dict) -> None:
    # event keys: type, message, source, date (+ progress)
    # type is "progress", "ok", "info", "warning", "error",
    # or (from the broker) "connected" / "disconnected"
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
