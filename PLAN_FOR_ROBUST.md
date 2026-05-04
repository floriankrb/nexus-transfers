# Robustness Plan — Remaining Items

---

## 1. Slow or Hanging Filesystem (NFS / Lustre)

**Affected operations**: `stat()`, `scandir()`, `open()`, `read()`,
`write()`, `rename()`.

### 1a. `list_dir` — `stat()` per entry

`dispatch.py` — `list_dir()` calls `entry.stat()` for every file in the
page when `include_size=True`.  On slow NFS each `stat()` can take
100 ms–1 s.  A directory with 60 000 files → 10 000 s in the worst case.

**Current**: runs in executor (event loop stays alive), progress bar
updates per entry.  No timeout on the overall operation.

**Fix**: wrap the executor call with `asyncio.wait_for()` and a
generous timeout (e.g. 10 minutes per page).  Log a clear error if
exceeded.

### 1b. File read during S3 upload

`s3.py` — `upload_file()` reads from disk in 8 MiB chunks.  On a hung
mount, `fh.read()` blocks the executor thread forever.

**Current**: no timeout.

**Fix**: same approach — timeout the executor call.

### 1c. File write on the receiving side

`client.py` — `_write_file()` writes to a temp file then `os.replace()`.
Both can block on a slow filesystem.

**Current**: runs in executor, no timeout.

**Fix**: timeout the executor call.  On timeout, delete the temp file.

### 1d. `os.path.getsize()` / `os.path.isfile()` in resume checks

`client.py` — the resume skip logic calls `getsize()` and `isfile()` for
every file.  Each can block on slow NFS.

**Current**: no timeout, runs on the event loop (not in executor).

**Fix**: move the check into the executor alongside the write, or batch
the stat calls.

---

## 4. Peer Unavailability and Slow Peers

### 4b. Infinite retry with no escalation

Workers retry on `PeerNotFoundError`, `ConnectionError`,
`TimeoutError` forever (`peer_retries=-1`).  There is no cumulative time
limit or exponential back-off.

**Current**: retries every `peer_delay` seconds indefinitely.

**Fix**: add exponential back-off (capped).  Optionally add a maximum
total retry time.  Log attempt count in warnings.

---

## 6. Temp File Orphans

`client.py` — `_write_file()` creates a temp file via `mkstemp()`.  On
`SIGKILL` or OS crash, the temp file is orphaned.

**Current**: cleaned up on Python exceptions but not on hard kills.

**Fix**: use a predictable temp-file naming convention (e.g.
`.<target>.tmp`) and clean up stale temp files at startup or before
writing.

---

## Distinction: Slow vs Hanging

A **slow** operation eventually completes — the system should wait
patiently (with progress feedback).

A **hanging** operation never completes — the system must detect this
and recover.

The key difference is a **timeout**.  Without timeouts, slow and hanging
are indistinguishable.  Current gaps:

| Operation              | Timeout?   | Slow behaviour        | Hang behaviour        |
|------------------------|------------|-----------------------|-----------------------|
| `list_dir` (stat)      | No         | Progress bar updates  | Executor blocks       |
| S3 upload              | No         | Progress bar updates  | Executor blocks       |
| S3 download            | No         | Progress bar updates  | Executor blocks       |
| File write             | No         | Executor blocks       | Executor blocks       |
| `send()` (WS send)     | 30 s       | Blocks event loop     | Times out             |
| `call_timeout`         | Optional   | Waits then retries    | Waits forever if None |

**Recommendation**: every I/O operation should have a timeout.  Timeouts
should be generous (minutes, not seconds) to accommodate slow systems
but finite to detect hangs.

---

## Priority

| #  | Item                                | Severity | Effort |
|----|-------------------------------------|----------|--------|
| 1  | Temp file cleanup (§6)              | LOW      | Low    |
| 2  | Exponential back-off (§4b)          | LOW      | Low    |
| 3  | Executor timeouts (§1a–d)           | MEDIUM   | Medium |
