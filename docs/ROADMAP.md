# Roadmap

Open items, checked against the code on 2026-09-28. Remove an item in the
change that implements it.

## Security

- **SSH host keys are not verified.** Every SSH connection (`ssh.py`, ~line
  105: `copy-ssh`, `check-files-ssh`, and every `api` call — `push_ssh`,
  `pull_ssh`, `verify_ssh`, `delete_ssh`, `stat_ssh`) uses
  `known_hosts=None`, with a warning. Verify
  against `~/.ssh/known_hosts` by default, with an explicit opt-out flag /
  config key.

## Robustness (from [prompts/PLAN_FOR_ROBUST.md](prompts/PLAN_FOR_ROBUST.md))

- **Slow / hanging filesystem timeouts.** No executor call has a timeout:
  `list_dir` scan and page `lstat`, S3 upload reads, file writes /
  `os.replace`, and the resume `isfile` / `getsize` checks (the latter still
  run on the event loop in `client/_transfer.py`). Wrap them in
  `asyncio.wait_for` with generous (minutes) limits; on a write timeout,
  delete the temp file.
- **Peer retry back-off.** `copy` / `check-files` workers retry
  `PeerNotFoundError` / `ConnectionError` / `TimeoutError` forever at a
  fixed `--peer-delay`. Add capped exponential back-off, optionally a total
  retry budget, and log the attempt count.
- **Orphan temp files.** Names are now predictable (`<name>.<8hex>.tmp`)
  and `check-files* --delete-extra` removes them, but nothing cleans them
  at startup or before writing after a `SIGKILL` / crash.

## Monitoring (from [prompts/MONITOR.md](prompts/MONITOR.md))

- **Retire the `task` descriptor.** `Client.monitor(task=…)`
  (`client/_client.py`, ~line 553) forwards a task object into the event,
  and `monitor.py` prints `task.name`; both are leftovers of the original
  design. Job identity now lives on the Nexus location row, and this repo
  has no job concept of its own. Drop the parameter (accept and ignore it
  for one release) and the printing; events keep `type`, `date`, `source`,
  `message` and `progress`.
- Still not implemented from the event schema: progress `uuid`, `start`,
  `update`, `minimum` (only `label`, `value`, `maximum`, `unit`, `rate` and
  command-specific counters are sent), per-RPC "message to peer" events
  other than `list_dir`, and a distinct "end" / registered-to-broker event
  type (clients send `ok` messages instead).

## Naming (from [prompts/NAMING.md](prompts/NAMING.md))

- Rename the `server` subcommand (runs a peer) to `peer`, keeping `server`
  as an alias; update `cli.py` help, `client/_interactive.py` and docs.
