# nexus-transfers — agent notes

- Reference docs are `README.md` (overview, install, quick start) and `docs/*.md` (USAGE, CLI, CONFIGURATION, PYTHON_API, PROTOCOL, BACKENDS, CHECK_FILES, ROADMAP). Update them in the same change as the code they describe; take facts from the code, not from older docs.
- `docs/prompts/` is history: the design prompts. Never edit a prompt's body; when it is implemented or superseded, add a one-line `> **Status (date):** …` banner at the top and index it in `docs/prompts/README.md`. Leftovers go to `docs/ROADMAP.md`.
- `src/nexus_transfers/api.py` (`push_ssh` / `pull_ssh` / `verify_ssh` / `delete_ssh` / `stat_ssh`, their async forms, `default_broker_url`) is a public contract used by anemoi-nexus-client (repo `/home/cloud-user/react-catalogue`). Any change to its signatures, `lock` semantics, progress-callback shape or result keys needs the matching update of that repo's `.agents/skills/transfers/SKILL.md` ("Byte transport: nexus-transfers" section).
- One script, `nexus-transfers <command>` (`cli.py`); the broker URL flag is `--broker-url`. Env prefix is `NEXUS_TRANSFERS_*` (S3: `NEXUS_TRANSFERS_S3_*`; the singular `NEXUS_TRANSFER_S3_*` is a deprecated alias in `config._DEPRECATED_ENV`).
- Every file write goes through a `<name>.<8hex>.tmp` + rename; `check-files --delete-extra` relies on that exact pattern.
