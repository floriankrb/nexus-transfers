# Configuration

Settings resolve with the precedence **CLI option > environment variable >
config file > built-in default**.

- Config: the `[nexus]` table of the anemoi settings,
  `~/.config/anemoi/settings.toml`, merged with the `[nexus]` table of
  `~/.config/anemoi/settings.secrets.toml` (which wins — put passwords and
  S3 keys there, mode 0600). `$ANEMOI_SETTINGS` (path of `settings.toml`;
  the secrets file sits next to it) or `$XDG_CONFIG_HOME` move them, as for
  anemoi-nexus-client, which reads `api_url` / `api_token` from the same
  table.
- The old `~/.config/nexus-transfers/settings.toml` and
  `~/.nexus-transfers.toml` are still read underneath (whole file, no
  `[nexus]` wrapper), with a deprecation warning.
- Invalid TOML is warned about and ignored.
- `~/.env` is loaded with `python-dotenv` at start-up, so the variables below
  can live there.

## Environment variables

All variables use the `NEXUS_TRANSFERS_` prefix (`NEXUS_TRANSFERS_S3_` for S3).
The old singular `NEXUS_TRANSFER_S3_*` names are still read when the new one
is unset, with a deprecation warning.

| Variable | Config key (in `[nexus]`) | Meaning |
|----------|------------------------|---------|
| `NEXUS_TRANSFERS_URL` | `url` | Default broker URL (`ws://localhost:8766` otherwise) |
| `NEXUS_TRANSFERS_USER` | `user` | HTTP Basic Auth user for the broker (with password) |
| `NEXUS_TRANSFERS_PASSWORD` | `password` | Basic Auth password; warned about over plain `ws://` |
| `NEXUS_TRANSFERS_S3_BUCKET` | `s3_bucket` | Staging bucket, `bucket` or `bucket/prefix` (`s3://` optional). Needed by the staging provider and by `check --s3`. |
| `NEXUS_TRANSFERS_S3_ENDPOINT_URL` | `s3_endpoint_url` | S3-compatible endpoint (default AWS) |
| `NEXUS_TRANSFERS_S3_ACCESS_KEY_ID` | `s3_access_key_id` | Access key (default: AWS credential chain) |
| `NEXUS_TRANSFERS_S3_SECRET_ACCESS_KEY` | `s3_secret_access_key` | Secret key (default: AWS credential chain) |
| `NEXUS_TRANSFERS_S3_VIRTUAL_HOSTED_STYLE` | `s3_virtual_hosted_style` | `1/true/yes/on`: virtual-hosted-style requests (only with an endpoint) |

## Sections

Each subcommand reads the defaults of its flags from its own sub-table of
`[nexus]` (written `[nexus.<section>]`), key =
flag name without `--`, dashes → underscores (`--max-concurrent` →
`max_concurrent`, `--broker-url` → `broker_url`, `--use-broker` →
`use_broker`):

| Section | Command |
|---------|---------|
| `[nexus.broker]` | `broker` |
| `[nexus.client]` | `server` |
| `[nexus.monitor]` | `monitor` (`--filter` → `filter`) |
| `[nexus.copy]` | `copy` |
| `[nexus.copy_ssh]` | `copy-ssh` |
| `[nexus.copy_s3]` | `copy-to-s3`, `copy-from-s3` |
| `[nexus.check_files]` | `check-files` |
| `[nexus.check_files_ssh]` | `check-files-ssh` |
| `[nexus.check_files_s3]` | `check-files-s3` (`--hash` → `hash`) |
| `[nexus.kill]` | `kill` (`broker_url` only) |

Not configurable: `--cipher`, `monitor --json`, the `check` flags and the
`kill` flags other than `broker_url`.

```toml
# ~/.config/anemoi/settings.toml
[nexus]
url = "wss://example.com/transfers"
s3_bucket = "my-bucket"
s3_endpoint_url = "https://s3.example.com"

[nexus.copy]
max_concurrent = 8
size = true

[nexus.copy_ssh]
broker_url = "wss://example.com/transfers"   # enables monitoring
ssh_connections = 4
processes = 4
```

```toml
# ~/.config/anemoi/settings.secrets.toml   (chmod 600)
[nexus]
password = "..."
s3_access_key_id = "..."
s3_secret_access_key = "..."
```

The broker URL follows the general order: `--broker-url` >
`NEXUS_TRANSFERS_URL` > the command's `[nexus.<section>] broker_url` > `[nexus]`
`url` > `ws://localhost:8766` (`config.broker_url_default`). The
monitoring-only commands (`copy-ssh`, `copy-*-s3`, `check-files-ssh`,
`check-files-s3`) have no built-in fallback: they monitor as soon as any of
those is set, and run without a broker when none is.
