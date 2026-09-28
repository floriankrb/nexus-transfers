# Wire protocol (v1)

Reference for the broker ↔ peer protocol, from `protocol.py`, `broker.py`,
`dispatch.py` and `client/_client.py`. The broker is a router: it decodes
the payload only of frames addressed to itself and forwards every other frame
verbatim.

## Frame

Every WebSocket message is one **binary** frame (text messages are ignored):

```
 byte  field
 ----  -----
  1    version       uint8, must be 1
  1    src_len       uint8
  N    source        sender name (UTF-8); "" for broker-originated frames
  1    msg_len       uint8
  M    msg_name      message type (UTF-8): "register", "call", "reply", "chunk", …
  1    tgt_len       uint8; 0 = the frame is addressed to the broker
  K    target        recipient peer name (UTF-8); absent when tgt_len == 0
  1    encoding      'J' = JSON payload, 'R' = raw bytes
  4    size          payload byte count, big-endian uint32
  P    payload
```

- Names are 1–255 bytes UTF-8, enforced by `encode_frame` and
  `decode_frame` (`ValueError`); the empty string is not a name but the
  sentinel for "the broker" (as `source`: sent by the broker, as `target`:
  addressed to it). `msg_name` is 1–255 bytes and never empty. `decode_frame`
  also rejects a truncated frame, trailing bytes after the payload and an
  encoding other than `J` / `R`; it returns the version as-is so the broker
  can answer a bad one.
- A frame whose version is not 1 gets an `error` back
  (`unsupported protocol version`); an undecodable frame is dropped.
- WebSocket `max_size` is 10 MiB on both broker and client; a peer closed
  with code 1009 (message too big) does not reconnect.
- Client keepalive: ping every 30 s, 60 s ping timeout.
- The broker trusts peers: a frame whose `source` differs from the
  registered name is still relayed; the mismatch is logged once per peer.

## Encodings

- **J** — UTF-8 JSON object. Every peer-to-peer `call` / `reply` /
  `kill` / `kill_ack` carries a `msg_id` (8 hex chars from a uuid4) used to
  match replies to pending futures.
- **R** — raw bytes, used only by `chunk`:
  ```
  [2 bytes: header length, big-endian][JSON header][raw chunk bytes]
  ```
  Header: `{"msg_id", "chunk", "total_chunks"}`, plus `"checksum"` (SHA-256
  hex of the whole file) on the last chunk.

## Broker-addressed messages (`tgt_len == 0`)

| msg_name | Direction | Payload |
|----------|-----------|---------|
| `register` | peer → broker | `{}`; `source` is the requested name. Must be the first frame. |
| `register` | broker → peer | `{}` ack. A re-`register` on a registered socket is acked again. |
| `register_monitor` | peer → broker / ack | `{}`; subscribes the socket to event broadcasts. |
| `list_clients` | peer → broker / reply | `{}` → `{"clients": [sorted names]}` |
| `monitor_event` | peer → broker | event object; broker fills `source` and `date` if missing and broadcasts it. |
| `monitor_event` | broker → monitors | the event (broker sends as source `""`), fire-and-forget, no reply. |
| `error` | broker → peer | `{"error": str, "msg_id"?: str}` |

Anything else addressed to the broker gets `error: unknown message '<name>'`.

Broker `error` strings (the client keys off them):

| Error | When | Client raises |
|-------|------|---------------|
| `must register first` | first frame is not `register` / empty source | `RuntimeError` on connect |
| `name '<n>' already taken` | name held by another socket | `NameTakenError` |
| `unknown target '<t>'` | target not registered (carries the `msg_id` if the payload is JSON) | `PeerNotFoundError` |
| `target '<t>' disconnected` | relay send to the target failed | `PeerNotFoundError` |
| `peer '<t>' disconnected before replying` | see pending calls below | `PeerNotFoundError` |

`PeerNotFoundError` is retried by `Client.send` up to `peer_retries`
(`-1` = forever) every `peer_delay` seconds, each time with a new `msg_id`.

## Peer frames (`tgt_len > 0`, relayed verbatim)

| msg_name | Enc | Payload |
|----------|-----|---------|
| `call` | J | `{"msg_id", "func", "args": [...], "kwargs"?: {...}}` |
| `reply` | J | `{"msg_id", "result"}` or `{"msg_id", "error", "traceback"}`; transfer replies add `binary_transfer` or `s3_transfer` (see below) |
| `chunk` | R | file chunk, layout above |
| `kill` | J | `{"msg_id", "reason", "from", "signal"}`; `signal` 9 = hard, 1 = soft |
| `kill_ack` | J | `{"msg_id", "result": "killed" \| "terminating", "from"}` |

A remote exception never kills the callee: it is returned as `error`
(`"<Type>: <message>"`) plus the formatted `traceback`, and raised as
`RemoteError` (with `.remote_traceback`) on the caller.

Incoming calls are dispatched as tasks (bounded at 4096 concurrent) and the
function runs in the default executor, so a slow `list_dir` or hash never
blocks the socket or its pings.

### Kill

The target sends `kill_ack` first, then:

- **hard (9)** — SIGKILLs registered child process groups (e.g. `copy-ssh
  --processes` shards) and `os._exit(1)`.
- **soft (1)** — SIGTERMs child groups, emits a `warning` event, closes the
  socket and exits 0.

`kill` is an ordinary peer frame; the broker does not track it.

## Pending calls and peer death

For each relayed **JSON** `call` with a `msg_id` the broker records
`(msg_id, caller)` under the callee; the callee's `reply` with the same
`msg_id` clears it. When a peer disconnects, the broker sends every caller
still waiting on it an `error` `peer '<name>' disconnected before replying`
with the original `msg_id`, so no call hangs on a dead peer (the caller then
retries per `peer_retries`). A reply that is followed by binary chunks
clears the entry at the reply; a peer dying mid-chunk-stream is caught by
the caller's `call_timeout`, if any.

On the client side, losing the broker connection fails every pending future
with `ConnectionError("connection lost")`, drops partial chunk buffers, and
reconnects (`reconnect_retries`, `-1` = forever, every `reconnect_delay`
seconds), re-subscribing as monitor if it was one. `NameTakenError` during a
reconnect is fatal: the peer has been displaced.

## Unique names as locks

The broker keeps names unique, so a name is a distributed mutex: exactly one
socket holds it. `claim.claim_name` (used by `--steal` on `copy`,
`copy-ssh`, `copy-to-s3` / `copy-from-s3`, and by `api.push_ssh(lock=…)`)
takes over a held name:

1. connect as `claim-<8hex>`, `list_clients`; free → return;
2. held → soft kill, wait up to 5 s for the name to drop;
3. still there → hard kill, wait up to 30 s in total, else `NameTakenError`.

The caller then registers under the name itself; losing a race there raises
`NameTakenError` (aborts under `--steal`). A displaced peer that tries to
reconnect gets `name already taken`, terminates its child workers and exits.
An S3 `reply` whose `msg_id` is not pending (addressed to a displaced
predecessor) is ignored rather than downloaded and cleaned up.

## RPCs (`dispatch.py`)

Always available: `adder(x)` → `x + 1`; `echo(*args)` → the argument (or the
list of arguments). With `allowed_paths` (`server --allow-path`) a peer also
exposes:

| RPC | Result |
|-----|--------|
| `list_dir(path=".", include_size=False, offset=0, limit=1000)` | list of `{"name", "type": "file" \| "dir", "size"?}` |
| `get_file(path, chunk_size=65536, use_s3=True, s3_prefix=None)` | chunked relay transfer or S3 staging (below) |
| `hash_file(path, algo="md5")` | `{"hash", "algo", "size", "mode"}` |
| `s3_cleanup(s3_key)` | deletes a staged object; only keys this peer uploaded |

Paths are checked by `resolve_safe_path`: any `..` component is rejected,
then the `realpath` must equal or lie under an allowed directory.

**`list_dir` paging** — the caller pages with `offset += len(page)` until a
page is shorter than `limit`. `offset == 0` takes a fresh sorted snapshot of
the directory names (a pure `scandir`, no stat); later pages are served from
that snapshot (LRU of 16 directories), so a walk is consistent even if the
directory changes. Each page entry is `lstat`ed; a vanished entry is still
returned as a `file` without size so the page is not short. Symlinks are
reported as `file`; `size` only for regular files and only with
`include_size`.

**`get_file`, relay mode** (`use_s3=False`) — the reply is
`{"binary_transfer": true, "result": {"size", "total_chunks", "name"}}`,
followed by `total_chunks` `chunk` frames. The receiver assembles them in
memory, hashes incrementally and fails with `checksum mismatch` if the
SHA-256 differs. A zero-byte file has no chunks and resolves to `b""`.

**`get_file`, S3 mode** (default) — the provider uploads and replies
`{"s3_transfer": true, "result": {"s3_key", "size", "checksum", "bucket"}}`;
see [BACKENDS.md](BACKENDS.md#s3-staging).

## Checksums

- Transfers (relay chunks, S3 staging): **SHA-256**, computed while
  streaming on both sides; a mismatch fails the file.
- `check-files*`: **md5** by default (`--algo` / `--hash`, any `hashlib`
  name) — corruption detection, not security.
- `copy-ssh`, `copy-to-s3` / `copy-from-s3` do not checksum; resume is by
  size. Use `check-files-ssh` / `check-files-s3` to verify.

## Monitor events

JSON objects, broadcast to every socket that sent `register_monitor`:

```json
{"type": "...", "date": "ISO-8601 UTC, ms", "source": "peer name",
 "message": "...", "progress": {...}}
```

Emitted by the **broker**: `connected` and `disconnected` (on register and
on socket close), with `type`, `date`, `source`, `message` only.

Emitted by **peers** via `Client.monitor(message, status=…, progress=…)`;
`type` is the status:

| type | Emitted on |
|------|------------|
| `ok` | connected, reconnected, copy / check complete (clean) |
| `info` | disconnecting |
| `progress` | copy / check start, periodic progress, `list_dir` served |
| `warning` | retry after a failed list/transfer, check progress with unfixed discrepancies, soft kill received |
| `error` | `Client` context exited with an exception, check finished with unfixed discrepancies |

Periodic progress is throttled to one event per 30 s. Copy `progress`
payload: `label`, `value` (bytes done incl. skipped), `unit: "byte"`,
`total_transferred`, `files_done`, `files_skipped`, `rate` (B/s), plus
`maximum` (total bytes) for `copy-ssh` / `copy-*-s3`. Check payload:
`label`, `value`, `maximum`, `unit: "file"`, `discrepancies`, `fixed`,
`skipped`. Progress `uuid` / `start` / `update` / `minimum` of the original
design are not emitted. The `task` object is not emitted either and is on its
way out: job identity belongs to the caller (a Nexus location row), not to the
relay ([ROADMAP](ROADMAP.md)).
