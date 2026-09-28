# Transfer backends

How bytes move, from the current code (`client/_transfer.py`,
`client/_client.py`, `client/_io.py`, `s3.py`, `copy_ssh.py`, `ssh.py`,
`copy_s3.py`). Four paths:

| Path | Command / API | Data path | Broker role |
|------|---------------|-----------|-------------|
| Relay | `copy --use-broker`, `get_file(use_s3=False)` | provider → broker → receiver (`chunk` frames) | data + control |
| S3 staging (default) | `copy`, `get_directory`, `get_file` | provider → S3 → receiver | control only |
| SSH push / pull | `copy-ssh`, `api.push_ssh`, `api.pull_ssh` | local ↔ SFTP ↔ remote host | optional monitoring / lock |
| Direct S3 | `copy-to-s3`, `copy-from-s3` | local ↔ S3 | optional monitoring / lock |

## Common rules

**Atomic writes.** No path ever writes the final file name directly. Every
writer stages to `<name>.<8 hex>.tmp` in the destination directory (same
filesystem) and renames:

| Writer | Rename |
|--------|--------|
| `client._io._write_file` (relay download) | `os.replace` |
| `s3.download_file` (staging and `copy-from-s3`) | `os.replace` by the caller |
| `ssh.write_file` (`copy-ssh`, `check-files-ssh --fix`) | SFTP `posix_rename` |
| `ssh.read_file` (`api.pull_ssh`) | `os.replace` |

On a Python exception the temp file is removed. After `SIGKILL` / a crash it
stays behind; `check-files* --delete-extra` removes exactly this pattern
(`<base>.<8hex>.tmp` whose `<base>` exists in the reference). Automatic
startup cleanup is not implemented ([ROADMAP](ROADMAP.md)). Uploads to S3 go
straight to the final key (a `PUT` is atomic).

**Permissions.** Local downloads and SFTP uploads are created `0644`
regardless of umask / server default.

**Resume.** Re-running a copy skips what is already there:

| Path | Skip when |
|------|-----------|
| `copy` (relay / S3) | the local file exists; with `--size` also its size equals the remote size |
| `copy-ssh` | remote `stat` size == local size |
| `api.pull_ssh` | the local file exists with the remote size |
| `copy-to-s3` | `HEAD` size == local size |
| `copy-from-s3` | local size == object size |

There is no content check on resume; run `check-files*` for that. The skip
count and bytes are logged and included in the summary and progress events.

**Monitoring.** In `copy-ssh`, `copy-to-s3` / `copy-from-s3`, `api.pull_ssh`
and `check-files*`, monitor events (relay and `on_monitor`) are best-effort
(`_run.Monitor`): a failed send is logged and never stops the work. The
exception is a lost name race under `--steal` / `lock=`, which aborts. The
worker pools (`_run.run_workers`) cancel all their other tasks on the first
failure and re-raise it unchanged.

**Retries.** In `copy`, listing and each file transfer retry forever on
`PeerNotFoundError`, `ConnectionError` or `TimeoutError`, sleeping
`--peer-delay` and emitting a `warning` event; `check-files` retries its
listing the same way (`client._transfer.walk_peer_dir`). S3 operations retry 3 times
with 1 s, 2 s back-off (a checksum mismatch is not retried). Broker
reconnects: `--reconnect-retries` / `--reconnect-delay`.

## Relay

`copy --use-broker`: the receiver walks the remote tree with paged
`list_dir` (1000 entries), queues files, and `--max-concurrent` workers call
`get_file(path, chunk_size=…, use_s3=False)`. The provider replies with the
size and chunk count, then streams `chunk` frames (`--chunk-size`, default
64 KiB) read in an executor. The receiver assembles the chunks **in memory**,
verifies SHA-256, and writes via `_write_file`. Frame limit is 10 MiB, so
keep `--chunk-size` well below that. See [PROTOCOL.md](PROTOCOL.md).

## S3 staging

The default for `copy` / `get_directory` / `get_file`.

1. The initiator (`copy`) builds a per-transfer prefix
   `YYYY-MM-DD-HHMMSS-<remote>-<name>-<uuid4>` (UTC; `<remote>` is `--from`,
   `<name>` the initiator's client name, by default `<site or "copy">-<8hex>`)
   and calls `get_file(path, use_s3=True, s3_prefix=…)` per file.
2. The provider uploads the file to `<prefix>/<absolute path without leading
   />`, hashing SHA-256 while streaming (8 MiB reads), and replies
   `{s3_key, size, checksum, bucket}`.
3. The initiator streams the object to `<target>.<8hex>.tmp` next to the
   destination, verifies the checksum, renames it into place.
4. The initiator calls `<provider>.s3_cleanup(key)`; the provider deletes
   the object (only keys it uploaded itself). The file on the provider's
   disk is untouched. Cleanup is also requested when the download fails.

Configuration (env var or config key, see
[CONFIGURATION.md](CONFIGURATION.md)):

- **Provider** needs `NEXUS_TRANSFERS_S3_BUCKET`. It may be `bucket` or
  `bucket/prefix` (`s3://` optional); the prefix is used only for keys
  without an explicit per-transfer prefix (i.e. not by `copy`).
- **Receiver** does not need the bucket (it comes in the reply) but builds
  its S3 client from its **own** `NEXUS_TRANSFERS_S3_ENDPOINT_URL`,
  `_ACCESS_KEY_ID`, `_SECRET_ACCESS_KEY` (falling back to the AWS default
  chain), so both sides must reach the same endpoint.
- `NEXUS_TRANSFERS_S3_VIRTUAL_HOSTED_STYLE` (`1/true/yes/on`) applies only
  when an endpoint is set; `http://` endpoints are allowed.

`nexus-transfers check --s3` round-trips a test object
(`<bucket prefix or "nexus-transfers">/check/<uuid>.txt`) to validate the
settings.

## SSH push

`copy-ssh` / `api.push_ssh` — no peer, no S3; the broker (if
`--broker-url`) only receives monitor events and holds the `--steal` lock.

- **Target** `[user@]host:/path`; no user = current user. The source may be a
  directory or a single file (then the target is the file itself).
- **Walk** — the local tree is scanned in an executor (`os.scandir`, no
  symlink following for directories). Empty directories are not created.
- **`SSHPool`** (`ssh.py`) opens `--ssh-connections` asyncssh connections,
  one SFTP client each, handed out round-robin. Reads `~/.ssh/config`
  (with `Include` expanded), `--ssh-key` or the agent / default keys.
  **Host keys are not verified** (`known_hosts=None`, warned once).
  Cipher preference (`--cipher`): `aes128-gcm@openssh.com`,
  `aes256-gcm@openssh.com`, `chacha20-poly1305@openssh.com`.
- **Pipeline** (`_run_shard`) — every file is `stat`ed on the remote at
  `--stat-concurrency` depth (default 64); files needing upload feed a queue
  drained by `--max-concurrent` upload workers. Upload = `makedirs`, `put`
  to `<name>.<8hex>.tmp`, `chmod 644`, `posix_rename`.
- **Processes** — `--processes N > 1` partitions the files into N shards
  balanced by bytes (largest first, to the lightest shard) and runs each in
  its own process with its own pool, spreading SSH encryption over cores.
  Children are process-group leaders registered with the client, so a kill
  of the parent (`kill`, `--steal`) also takes the shards down.
- **Progress** — a `progress` event every 30 s (bytes incl. skipped,
  `maximum` = total bytes); `api.push_ssh` additionally calls
  `progress(bytes_done, bytes_total, files_done)` every
  `progress_interval` s and once at the end; raising from it aborts the copy.
- **Lock** — with `--steal` (or `push_ssh(lock=…)` plus a broker) the client
  name is claimed first (soft then hard kill of the holder); losing the name
  aborts. Without `--steal`, a name clash only disables monitoring.
- Result (`push_ssh`): `{bytes, files, transferred_bytes, transferred_files,
  skipped_bytes, skipped_files}`. `stat_ssh(target)` walks the remote over one
  connection: `{bytes, files}` or `None`.
- **The other `api` calls** (`ssh_ops.py`, `check_files_ssh._verify_ssh`;
  no CLI) reuse the pool: `pull_ssh` walks the remote (`walk_remote`),
  skips local files of the remote size and downloads the rest with
  `--max-concurrent` workers (`ssh.read_file`: `get` to
  `<name>.<8hex>.tmp`, `chmod 644`, `os.replace`), with the same lock and
  progress as the push. `verify_ssh` compares one remote walk with a local
  reference or a `{rel: size}` manifest (sizes; with `checksum`, `md5sum`
  of same-size files). `delete_ssh` checks the path first (absolute, ≥ 3
  components, inside `root` and not it), then removes files and symlinks
  and the directories deepest first. For all of them `progress` raising
  aborts the work and propagates (`_progress.run_abortable`).

`--ssh-connections` vs `--max-concurrent`: SSH multiplexes channels over one
TCP stream limited by its flow-control window, so several connections can
fill a fast, high-latency link that one cannot; the upload workers are spread
over them.

## Direct S3

`copy-to-s3 --source <local file|dir> --target s3://bucket[/prefix]`,
`copy-from-s3 --source s3://bucket/key-or-prefix --target <local>`.
Credentials / endpoint from `NEXUS_TRANSFERS_S3_*`; the URL's bucket replaces
`NEXUS_TRANSFERS_S3_BUCKET`.

- Keys: `<prefix>/<relative path>`. A single-file upload goes to
  `<prefix>/<basename>` if the prefix is empty or ends in `/`, else exactly
  to `<prefix>`.
- Download: an object at exactly the given key wins over a prefix listing;
  a prefix is listed page by page (1000 keys). Directory-marker objects are
  skipped; empty directories do not exist on S3 and are not recreated.
- Pipeline mirrors `copy-ssh`: a producer lists the source in batches, a
  classifier checks the target at `--stat-concurrency`, `--max-concurrent`
  (default 8) workers transfer. No checksum (use `check-files-s3 --hash`).
- `--steal` claims the name before connecting, but unlike `copy-ssh` a lost
  race afterwards only disables monitoring; it does not abort.
