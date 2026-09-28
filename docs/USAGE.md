# Using the commands

Task-oriented guide to the transfer, check, kill and monitor commands. Every
flag is listed in [CLI.md](CLI.md); settings files and environment variables
are in [CONFIGURATION.md](CONFIGURATION.md).

## Direct SSH copy (no relay required)

`nexus-transfers copy-ssh` copies a local directory (or a single file)
straight to a remote host over SFTP. No `nexus-transfers server` on the
remote side, no S3, no relay for data — the relay is used only to send
progress messages to a monitor peer.

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
    --ssh-connections 2 \
    --processes 4
```

Interrupted transfers resume automatically: a file is skipped when its remote
size already matches the local size. Each file is uploaded to
`<name>.<8hex>.tmp` and renamed into place, so a partial file never carries
the final name. `--processes N` shards the files (balanced by bytes) over N
worker processes to spread SSH encryption over cores. SSH host keys are
**not** verified. See [BACKENDS.md](BACKENDS.md#ssh-push).

## Direct S3 copy (no relay required)

`nexus-transfers copy-to-s3` and `nexus-transfers copy-from-s3` copy a local
file or directory straight to/from an S3 bucket using the
`NEXUS_TRANSFERS_S3_*` credentials (the `s3://bucket/...` argument overrides
only the bucket name). No peer or relay is involved in the data path — as
with `copy-ssh`, the relay is used only for optional progress monitoring.

```bash
nexus-transfers copy-to-s3 \
    --source /data/dataset.zarr \
    --target s3://my-bucket/datasets/dataset.zarr

nexus-transfers copy-from-s3 \
    --source s3://my-bucket/datasets/dataset.zarr \
    --target /data/dataset.zarr
```

Interrupted transfers resume automatically (files whose size already matches
are skipped). Empty directories are not represented on S3, so they are not
recreated on download.

## S3 staging

S3 staging is the **default** transfer mode of `copy` (and of
`get_file` / `get_directory`). Configure the bucket on the providing client
(the receiving side learns the bucket name from the provider's reply, but
uses its own endpoint and credentials, so set those on both sides):

```bash
export NEXUS_TRANSFERS_S3_BUCKET=my-bucket                      # provider only
export NEXUS_TRANSFERS_S3_ENDPOINT_URL=https://s3.example.com   # optional
export NEXUS_TRANSFERS_S3_ACCESS_KEY_ID=...                     # optional
export NEXUS_TRANSFERS_S3_SECRET_ACCESS_KEY=...                 # optional
nexus-transfers check --s3                                     # test them
```

Then:

```bash
nexus-transfers copy --from a /remote/path ./local-path
```

To bypass S3 and send the data over the WebSocket relay instead:

```bash
nexus-transfers copy --from a /remote/path ./local-path --use-broker
```

Flow: provider uploads to `<YYYY-MM-DD-HHMMSS>-<remote>-<name>-<uuid4>/<absolute path>`
→ returns key/size/sha256/bucket → initiator downloads from S3 to a temp
file, verifies, renames → initiator tells provider to delete the staged
object. The file on the provider's disk is untouched. Details in
[BACKENDS.md](BACKENDS.md#s3-staging).

Resume: `copy` skips a file that already exists locally; with `--size` it
also requires the size to match the remote one.

## Integrity check

`nexus-transfers check-files` verifies a local copy against a remote nexus
reference (hashes are computed on each side; no file content is transferred),
and `nexus-transfers check-files-ssh` verifies a remote SSH copy against the
local reference. Both detect corruption, missing files, extra files and
permission drift, exit non-zero on unfixed discrepancies, and can repair with
`--fix`, `--delete-extra` and `--fix-permissions MODE` (explicit octal mode,
e.g. `600`). See [CHECK_FILES.md](CHECK_FILES.md).

`nexus-transfers check-files-s3` verifies an S3 copy against the local
reference: sizes are compared by default (one bucket listing, no data
transfer); `--hash md5` streams every object back and compares digests.

```bash
nexus-transfers check-files --from a /remote/path ./local-path --fix
nexus-transfers check-files-ssh --source /data --target user@host:/remote --fix
nexus-transfers check-files-s3 --source /data --target s3://bucket/prefix --fix
```

Exit status: 0 clean (or fully fixed), 1 unfixed discrepancies, 2 the check
refused to run (e.g. empty or missing reference).

## Locks, kill and monitoring

**Names are locks.** The broker keeps client names unique, so a name is a
distributed mutex. `--steal` (on `copy`, `copy-ssh`, `copy-to-s3`,
`copy-from-s3`) displaces a client already registered under `--name` — soft
kill, then hard kill if it does not exit — and takes the name over. With a
name keyed on the unit of work (e.g. `nexus-location-<location_uuid>`, the Nexus client's lock) at most one
run of that work is active. `copy-ssh --steal` requires `--broker-url` and
aborts if the name cannot be claimed.

**Kill.**

```bash
nexus-transfers kill copy-1a2b3c4d           # exact name
nexus-transfers kill 'copy-*'                # fnmatch wildcard (quote it)
nexus-transfers kill --all --dry-run         # list only
nexus-transfers kill 'nexus-location-*' --sweep 30 --every 2
```

`-1` / `--soft`: the target closes cleanly and exits 0. `-9` / `--hard`: it
exits immediately, abandoning transfers. Neither: soft, wait `--grace` s,
then hard for survivors. Child worker processes (`copy-ssh --processes`) are
killed with their parent. `monitor-*` clients and the killer itself are
skipped unless `--include-monitors`. `--sweep` repeats the pass to catch
workers that were mid-reconnect. `kill` connects to `NEXUS_TRANSFERS_URL`
(it has no `--broker-url`).

**Monitor.**

```bash
nexus-transfers monitor --broker-url wss://example.com/transfers
nexus-transfers monitor --filter 'nexus-location-*'   # only matching sources
nexus-transfers monitor --json                   # raw event JSON
```

Prints the connected clients, then every broadcast event: time, type
(`connected`, `disconnected`, `ok`, `info`, `progress`, `warning`,
`error`), source, message, progress. Event format:
[PROTOCOL.md](PROTOCOL.md#monitor-events).
