# CLI reference

`nexus-transfers <command> --help` lists every flag. Flag defaults can also
come from the environment and the config file: see
[CONFIGURATION.md](CONFIGURATION.md). For worked examples see
[USAGE.md](USAGE.md).

Connection flags shared by `server`, `copy`, `check-files` and `monitor`
(`monitor` and `check-files` take the subset noted in their tables):

| Flag | Default | Description |
|------|---------|-------------|
| `--broker-url` | `$NEXUS_TRANSFERS_URL`, else the command's `[nexus.<section>] broker_url`, else `[nexus] url`, else `ws://localhost:8766` | Broker WebSocket URL |
| `--reconnect-retries` | `-1` (forever) | Reconnection attempts when the broker connection drops; `0` disables |
| `--reconnect-delay` | `2.0` | Seconds between reconnection attempts |
| `--peer-retries` | `-1` (forever) | Retries of a call whose target peer is not registered (or died before replying) |
| `--peer-delay` | `2.0` | Seconds between peer retries (also the retry sleep of `copy` workers) |
| `--call-timeout` | none | Seconds to wait for an RPC reply |
| `--no-verify` | off | Skip TLS certificate verification for `wss://` |
| `--debug` | off | Debug logging |

## `nexus-transfers broker`

| Flag     | Default     | Description   |
|----------|-------------|---------------|
| `--host` | `localhost` | Bind address  |
| `--port` | `8766`      | Bind port     |
| `--debug`| off         | Debug logging |

## `nexus-transfers server`

| Flag            | Default                  | Description                                      |
|-----------------|--------------------------|--------------------------------------------------|
| `--name`        | (required)               | Unique client ID                                 |
| `--allow-path`  | (none)                   | Directory to expose for file operations (repeatable) |
| `--interactive` | off                      | Start an interactive prompt instead of a headless worker |
| connection flags | | all of the above |

## `nexus-transfers monitor`

| Flag | Default | Description |
|------|---------|-------------|
| `--name` | `monitor-<6hex>` | Client name |
| `--filter PATTERN` | (none) | Only events whose source matches the fnmatch pattern; the client listing is filtered too |
| `--json` | off | Print raw JSON events |
| `--broker-url`, `--reconnect-retries`, `--reconnect-delay`, `--no-verify`, `--debug` | | connection flags |

## `nexus-transfers copy`

| Flag               | Default               | Description                                            |
|--------------------|-----------------------|--------------------------------------------------------|
| `--from`           | (required)            | Name of the remote client                              |
| `source target`    | (required)            | Remote source dir, local target dir                    |
| `--name`           | `<site or copy>-<8hex>` | Client name on the broker                            |
| `--site`           | (none)                | Label replacing `copy` in the generated name; shown as `site:target` |
| `--max-concurrent` | `4`                   | Maximum parallel file transfers                        |
| `--chunk-size`     | `65536`               | Binary chunk size (only used with `--use-broker`)      |
| `--use-broker`     | off                   | Send data over the relay instead of S3 staging (S3 is the default and needs `NEXUS_TRANSFERS_S3_*`) |
| `--size`           | off                   | Byte progress; resume also checks sizes (asks the remote for sizes) |
| `--steal`          | off                   | Take over `--name` from a registered holder (soft, then hard kill) |
| connection flags   |                       | all of them                                            |

## `nexus-transfers copy-ssh`

| Flag               | Default                | Description                                         |
|--------------------|------------------------|-----------------------------------------------------|
| `--source`         | (required)             | Local directory (or file) to copy                   |
| `--target`         | (required)             | `[user@]host:/remote/path`                          |
| `--broker-url`     | (none — monitoring disabled) | Relay URL for monitoring only (optional)      |
| `--name`           | `<site or ssh-copy>-<8hex>` | Client name on the relay                       |
| `--site`           | (none)                 | Site label for monitor messages                     |
| `--max-concurrent` | `4`                    | Number of parallel SFTP uploads (per process)       |
| `--ssh-port`       | `22`                   | SSH port on the target host                         |
| `--ssh-key`        | SSH agent / default    | Path to private key file                            |
| `--ssh-connections`| `2`                    | Number of SSH connections to open per process (see note below) |
| `--processes`      | `1`                    | Worker processes to shard files across (spreads SSH encryption over cores) |
| `--stat-concurrency` | `64`                 | Concurrent remote `stat` calls during the resume scan |
| `--cipher ALG …`   | `aes128-gcm@openssh.com aes256-gcm@openssh.com chacha20-poly1305@openssh.com` | SSH cipher preference list |
| `--size`           | off                    | Show byte-based progress instead of file count      |
| `--steal`          | off                    | Take over `--name` (requires `--broker-url`); abort if it cannot be claimed |
| `--no-verify`      | off                    | Skip TLS verification for the relay connection      |
| `--debug`          | off                    | Enable debug logging                                |

**`--ssh-connections` vs `--max-concurrent`**: `--ssh-connections` controls how
many TCP connections are opened to the SSH server.  Each connection carries one
SFTP session, and the `--max-concurrent` upload workers are distributed across
those sessions in round-robin order.  Opening more than one connection lets
multiple SFTP sessions run in parallel, which can saturate bandwidth that a
single SSH connection cannot fully use (SSH multiplexes all channels over one
TCP stream, so a single connection is limited by its flow-control window).  Two
connections is a reasonable default; raise it if the link is fast and latency is
high. `--processes` multiplies both.

## `nexus-transfers copy-to-s3` / `copy-from-s3`

| Flag               | Default                | Description                                         |
|--------------------|------------------------|-----------------------------------------------------|
| `--source`         | (required)             | Local path (`copy-to-s3`) or `s3://bucket/key-or-prefix` (`copy-from-s3`) |
| `--target`         | (required)             | `s3://bucket[/prefix]` (`copy-to-s3`) or local path (`copy-from-s3`) |
| `--broker-url`     | (none — monitoring disabled) | Relay URL for monitoring only (optional)      |
| `--name`           | `<site or s3-copy>-<8hex>` | Client name on the relay                        |
| `--site`           | (none)                 | Site label for monitor messages                     |
| `--max-concurrent` | `8`                    | Number of parallel S3 transfers                     |
| `--stat-concurrency` | `64`                 | Concurrent target existence/size checks during the resume scan |
| `--size`           | off                    | Show byte-based progress instead of file count      |
| `--quiet`          | off                    | Suppress console output                             |
| `--steal`          | off                    | Take over `--name` (requires `--broker-url`); abort if it cannot be claimed |
| `--no-verify`      | off                    | Skip TLS verification for the relay connection      |
| `--debug`          | off                    | Enable debug logging                                |

## `nexus-transfers check`

| Flag | Description |
|------|-------------|
| `--s3` | Put / get / delete a small object on `NEXUS_TRANSFERS_S3_BUCKET` (at `<bucket prefix or nexus-transfers>/check/<uuid>.txt`), verify its SHA-256; prints the resolved settings with secrets redacted |
| `--site NAME` | Resolve the site's `broker_url` from the anemoi registry, then connect and register to it |
| `--no-verify` | Skip TLS verification when dialling the broker (`--no-ssl-verify` is a deprecated alias) |
| `-v`, `--verbose` | Full tracebacks |

At least one of `--s3` / `--site`. Exit 0 on success, 1 on failure, 2 when
the bucket is not set.

## `nexus-transfers kill`

| Flag | Default | Description |
|------|---------|-------------|
| `pattern` | | Client name or fnmatch wildcard |
| `--all` | | Every client (same as `'*'`) |
| `-1`, `--soft` | | Soft kill only |
| `-9`, `--hard` | | Hard kill only |
| `--grace SECS` | `2.0` | Wait between soft and hard kill (default mode only) |
| `--sweep SECS` | `0` | Repeat list-and-kill for this long |
| `--every SECS` | `2.0` | Seconds between sweep passes |
| `--broker-url` | `$NEXUS_TRANSFERS_URL`, else `[nexus.kill] broker_url` / `[nexus] url`, else `ws://localhost:8766` | Broker WebSocket URL |
| `--no-verify` | off | Skip TLS certificate verification for `wss://` |
| `--reason` | `killed via nexus-transfers kill` | Logged by the target |
| `--include-monitors` | off | Also kill `monitor-*` clients |
| `--dry-run` | off | List targets only |
| `-v`, `--verbose` | off | Debug logging |

Exit 0 when every target acknowledged (or, with `--sweep`, none is left), 1
otherwise.

## `nexus-transfers check-files`

| Flag | Default | Description |
|------|---------|-------------|
| `--from` | (required) | Remote peer holding the reference |
| `source target` | (required) | Remote reference dir, local dir to verify |
| `--name` | `[<site>-]<8hex>-check` | Client name |
| `--site` | (none) | Label used in the generated name |
| `--algo` | `md5` | Hash algorithm (`hashlib` name) |
| `--fix` | off | Re-download corrupt or missing files |
| `--delete-extra` | off | Delete whitelisted extras only (`<base>.<8hex>.tmp` leftovers, `_build/`) |
| `--fix-permissions MODE` | (none) | Force this octal mode on every local file |
| `--max-concurrent` | `4` | Parallel file checks |
| `--max-age AGE` | (none) | Only check files modified within `AGE` (`30d`, `1h`, `45m`, `2w`, seconds) |
| `--use-broker` | off | Fix downloads via the relay instead of S3 staging |
| `--chunk-size` | `65536` | Relay chunk size for fix downloads |
| `--broker-url`, `--peer-retries`, `--peer-delay`, `--call-timeout`, `--no-verify`, `--debug` | | connection flags |

## `nexus-transfers check-files-ssh`

| Flag | Default | Description |
|------|---------|-------------|
| `--source` | (required) | Local reference directory |
| `--target` | (required) | Remote copy: `[user@]host:/remote/path` |
| `--algo` | `md5` | Hash algorithm; the remote needs `<algo>sum` |
| `--fix`, `--delete-extra`, `--fix-permissions MODE`, `--max-age AGE` | | as `check-files` (re-upload instead of download) |
| `--max-concurrent` | `4` | Parallel file checks |
| `--ssh-port`, `--ssh-key`, `--ssh-connections`, `--cipher` | `22`, agent, `2`, GCM first | as `copy-ssh` |
| `--broker-url` | (none — monitoring disabled) | Relay URL for monitoring only |
| `--name`, `--site`, `--no-verify`, `--debug` | | as `copy-ssh` |

## `nexus-transfers check-files-s3`

| Flag               | Default                | Description                                         |
|--------------------|------------------------|-----------------------------------------------------|
| `--source`         | (required)             | Local reference directory                           |
| `--target`         | (required)             | S3 copy to verify: `s3://bucket[/prefix]`           |
| `--hash`           | (none — sizes only)    | Hash algorithm (e.g. `md5`); re-downloads every byte |
| `--fix`            | off                    | Re-upload corrupt or missing objects                |
| `--delete-extra`   | off                    | Delete objects not in the local reference (whitelisted extras only) |
| `--max-concurrent` | `8`                    | Maximum parallel file checks                        |
| `--broker-url`     | (none — monitoring disabled) | Relay URL for monitoring only (optional)      |
| `--name`           | auto-generated         | Client name on the relay                            |
| `--site`           | (none)                 | Site label for monitor messages                     |
| `--no-verify`      | off                    | Skip TLS verification for the relay connection      |
| `--debug`          | off                    | Enable debug logging                                |
