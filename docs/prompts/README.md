# Prompts

The prompts this package was built from. They are **history**, not reference:
the reference is the [README](../../README.md) and the other files in
[docs/](..). Command and flag names in the prompts (`nexus-copy`,
`nexus-client`, `nexus-monitor`, `--server-url`, …) are historical.

Rule: when a prompt is implemented or superseded, add a one-line status
banner at the top (`> **Status (YYYY-MM-DD):** …`) pointing at the reference
and, for leftovers, at [../ROADMAP.md](../ROADMAP.md). **Never edit the
body.**

| File | Gist | Status |
|------|------|--------|
| [PROMPT.md](PROMPT.md) | Original build log: relay, RPC dispatch, binary `get_file`, `list_dir`, `get_directory`, checksums, Basic Auth, copy CLI | implemented |
| [PROTOCOL.md](PROTOCOL.md) | Binary frame layout, broker decodes only its own frames, chunk size option | implemented |
| [ERRORS.md](ERRORS.md) | Broker tracks unreplied calls, errors the caller on peer death | implemented |
| [ROBUST.md](ROBUST.md) | Atomic temp-file writes, resume, reconnect / peer retries, call timeout | implemented |
| [PLAN_FOR_ROBUST.md](PLAN_FOR_ROBUST.md) | Remaining robustness gaps: FS timeouts, back-off, orphan temp files | partly — rest in ROADMAP |
| [S3_TRANSFER.md](S3_TRANSFER.md) | S3 staging of `get_file`, per-transfer prefix, `--site`, `--size` | implemented (now default) |
| [TRANSFER_SSH.md](TRANSFER_SSH.md) | Direct local → SFTP copy with relay-only monitoring | implemented as `copy-ssh` |
| [CONFIG.md](CONFIG.md) | TOML config, env → config key mapping, precedence; naming question | implemented |
| [CLI.md](CLI.md) | Single `nexus-transfers <command>` entry point | implemented |
| [MONITOR.md](MONITOR.md) | Monitor registration, broadcast events, event schema, `on_monitor` | partly — schema rest in ROADMAP |
| [NAMING.md](NAMING.md) | broker / peer / source / destination naming analysis | partly — rest in ROADMAP |
