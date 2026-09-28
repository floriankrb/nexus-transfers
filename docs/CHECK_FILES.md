# File integrity check (`check-files` / `check-files-ssh` / `check-files-s3`)

Verify a transferred tree against its reference: detect data corruption,
missing files, extra files and permission drift — and optionally fix them.

There are three modes, mirroring the copy commands:

| Command           | Reference            | Verified copy        | Transport |
|-------------------|----------------------|----------------------|-----------|
| `check-files`     | remote nexus client  | local directory      | relay     |
| `check-files-ssh` | local directory      | remote SSH directory | asyncssh  |
| `check-files-s3`  | local directory      | S3 prefix            | obstore   |

In relay and SSH mode no file content crosses the wire during a check: each
side hashes its own copy (md5 by default — corruption detection, not
security) and only the digests are compared. Content is only transferred
when `--fix` re-downloads or re-uploads a bad file. S3 has no server-side
hash, so S3 mode compares sizes by default and streams objects back only
with `--hash`.

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
`nexus-transfers copy` (1000 entries per page). For every file the client
calls the `hash_file` RPC — the remote peer computes the digest locally and returns
`{hash, algo, size, mode}` — while hashing its own copy concurrently
(`--max-concurrent`, default 4). The local tree is then walked to detect
files absent from the reference.

Fix downloads use the same path as `nexus-transfers copy`: S3 staging by
default, `--use-broker` (with `--chunk-size`) for chunked relay transfer. A
re-downloaded file gets the reference's mode. The peer must run a version
that exposes `hash_file`.

`--max-age AGE` restricts the check to files on the checked side modified
within the given duration (`30d`, `1h`, `45m`, `2w`, or a bare number of
seconds); older files are skipped and counted separately in the summary.
In relay mode the local copy's mtime is used; in SSH mode the remote
copy's (one cheap stat replaces the remote hash for skipped files).
`check-files-s3` has no `--max-age`.
Missing files are always reported — they have no age.

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
connections used by `copy-ssh` (`--ssh-connections`, `--ssh-port`,
`--ssh-key`, `--cipher`). The remote tree is walked over SFTP, before
the files are compared, to detect extra files. Fixes re-upload with the same atomic
tmp-file + rename used by `copy-ssh`; `--broker-url` optionally enables
relay monitoring exactly like `copy-ssh`.

## S3 mode

```bash
# The local directory is the reference; verify the S3 copy by size.
nexus-transfers check-files-s3 --source /data/dataset.zarr \
    --target s3://bucket/datasets/dataset.zarr

# Re-download every object and compare md5; repair.
nexus-transfers check-files-s3 --source /data/dataset.zarr \
    --target s3://bucket/datasets/dataset.zarr --hash md5 --fix --delete-extra
```

The prefix is listed once (sizes come with the listing). Without `--hash`
only sizes are compared — no data is transferred. With `--hash ALGO` every
object is streamed back and hashed. `--fix` re-uploads with the
`copy-to-s3` credentials (`NEXUS_TRANSFERS_S3_*`; the URL's bucket wins).

## Behaviour

- **Default**: discrepancies are reported (console + monitor channel) and
  the command exits with status 1 — it fails loudly, nothing is modified.
- `--fix`: corrupt and missing files are transferred again from the
  reference.
- `--delete-extra`: deliberately narrow — it only deletes whitelisted
  extras: (a) debris from an interrupted transfer — the name ends in
  `.<8 hex>.tmp` (the staging name of every atomic write, see
  [BACKENDS.md](BACKENDS.md#common-rules)) **and** the corresponding base
  file exists on the reference; (b) anything under the dataset's top-level `_build/`
  directory (scratch space from dataset creation). Any other extra file is
  reported but never deleted, whatever the options. A deletion that fails is
  logged and the extra stays in the report, unfixed.
- Safety invariants (always on): the check refuses to run when the
  reference contains no files or (SSH / S3 mode) the `--source` directory
  is missing — an empty reference is far more likely a wrong path or a
  half-mounted filesystem than a real dataset, and deleting "extras"
  against it would wipe the copy. Exit code 2, nothing touched.
- `--fix-permissions MODE`: every file on the checked side is forced to the
  given octal mode (e.g. `--fix-permissions 600`); there is no default —
  without this option permission drift against the reference is only
  reported, never fixed. Not available in S3 mode (objects have no mode).
- Exit status is 0 only when no discrepancy remains unfixed.

## Monitoring

Events go to the monitor channel like the copy commands, grouped to at
most one message per 30 seconds:

- start: `…: starting check <copy> against <reference>`
- progress (throttled): `…: checked N/M files in <label>: 2 corrupt, 1 extra (3 fixed)`
- final summary (always sent): `…: check of <label> finished — N files in Xs, <counts>`
  with status `ok` when clean/fully fixed, `error` otherwise.

## Options

Shared: `--fix`, `--delete-extra`, `--max-concurrent` (default 4; 8 for
S3), `--broker-url`, `--name`, `--site`, `--no-verify`, `--debug`.
`check-files` / `check-files-ssh` add `--algo` (any `hashlib` name; SSH
mode needs a matching `<algo>sum` binary on the remote host),
`--fix-permissions MODE` and `--max-age AGE`; `check-files` adds
`--use-broker`, `--chunk-size`, `--peer-retries`, `--peer-delay`,
`--call-timeout`; `check-files-s3` has `--hash ALGO` instead of `--algo`.
In SSH and S3 mode `--broker-url` is optional (monitoring only). All
options also resolve through the TOML config sections `check_files`,
`check_files_ssh`, `check_files_s3` (see
[CONFIGURATION.md](CONFIGURATION.md)).
