# File integrity check (`check-files` / `check-files-ssh`)

Verify a transferred tree against its reference: detect data corruption,
missing files, extra files and permission drift — and optionally fix them.

There are two modes, mirroring the two copy commands:

| Command           | Reference            | Verified copy        | Transport |
|-------------------|----------------------|----------------------|-----------|
| `check-files`     | remote nexus client  | local directory      | relay     |
| `check-files-ssh` | local directory      | remote SSH directory | asyncssh  |

No file content crosses the wire during a check: each side hashes its own
copy (md5 by default — corruption detection, not security) and only the
digests are compared. Content is only transferred when `--fix` re-downloads
or re-uploads a bad file.

## Relay mode

```bash
# The remote nexus client "hpc-a" is the reference.
nexus-transfers check-files --from hpc-a /data/dataset.zarr /local/dataset.zarr \
    --broker-url wss://example.com/transfers

# Repair mode: re-download bad/missing files, delete local strays,
# force permissions to 600.
nexus-transfers check-files --from hpc-a /data/dataset.zarr /local/dataset.zarr \
    --fix --delete-extra --fix-permissions 600
```

The remote tree is walked with the same paged `list_dir` RPC as
`nexus-copy` (1000 entries per page). For every file the client calls the
new `hash_file` RPC — the server computes the digest locally and returns
`{hash, algo, size, mode}` — while hashing its own copy concurrently
(`--max-concurrent`, default 4). The local tree is then walked to detect
files absent from the reference.

Fix downloads use the same path as `nexus-copy`: S3 staging by default,
`--use-broker` for chunked relay transfer.

## SSH mode

```bash
# The local directory is the reference; verify the remote copy.
nexus-transfers check-files-ssh --source /data/dataset.zarr \
    --target user@host:/remote/dataset.zarr

# Repair mode.
nexus-transfers check-files-ssh --source /data/dataset.zarr \
    --target user@host:/remote/dataset.zarr --fix --delete-extra --fix-permissions 600
```

Local files are walked and hashed in a thread pool; the remote digest is
computed by running `<algo>sum` (default `md5sum`) over the pooled asyncssh
connections used by `nexus-copy-ssh`. The remote tree is walked over SFTP
to detect extra files. Fixes re-upload with the same atomic
tmp-file + rename used by `copy-ssh`; `--broker-url` optionally enables
relay monitoring exactly like `copy-ssh`.

## Behaviour

- **Default**: discrepancies are reported (console + monitor channel) and
  the command exits with status 1 — it fails loudly, nothing is modified.
- `--fix`: corrupt and missing files are transferred again from the
  reference.
- `--delete-extra`: files present on the checked side but not on the
  reference are deleted (not an error).
- `--fix-permissions MODE`: every file on the checked side is forced to the
  given octal mode (e.g. `--fix-permissions 600`); there is no default —
  without this option permission drift against the reference is only
  reported, never fixed.
- Exit status is 0 only when no discrepancy remains unfixed.

## Monitoring

Events go to the monitor channel like the copy commands, grouped to at
most one message per 30 seconds:

- start: `…: starting check <copy> against <reference>`
- progress (throttled): `…: checked N/M files in <label>: 2 corrupt, 1 extra (3 fixed)`
- final summary (always sent): `…: check of <label> finished — N files in Xs, <counts>`
  with status `ok` when clean/fully fixed, `error` otherwise.

## Options shared by both commands

`--algo` (any `hashlib` name; SSH mode needs a matching `<algo>sum` binary
on the remote host), `--fix`, `--delete-extra`, `--fix-permissions MODE`,
`--max-concurrent`, `--name`, `--site`, `--no-verify`, `--debug`. All
options also resolve through the usual TOML config sections
(`check_files`, `check_files_ssh`).
