Refactor the nexus CLIs into a single one called "nexus-transfers" followed by a single word and then arguments

- broker (run the broker, runs forever)
- monitor (run a monitor, runs forever)
- server (run a peer awaiting for messages, runs forever)

Others will be instantiation of a peer that will initiate messages to servers, and terminate.
For example "nexus-transfers copy ..."

## Implemented

The unified CLI is in `src/nexus_transfers/cli.py`. Entry point: `nexus-transfers`.

```
nexus-transfers broker     — Run the WebSocket relay broker (runs forever)
nexus-transfers monitor    — Run a monitoring client (runs forever)
nexus-transfers server     — Run a peer awaiting messages (runs forever)
nexus-transfers copy       — Copy a remote directory to local via relay (terminates)
nexus-transfers copy-ssh   — Copy a local directory to a remote SSH target (terminates)
nexus-transfers copy-to-s3   — Copy a local file or directory to an S3 bucket (terminates)
nexus-transfers copy-from-s3 — Copy an S3 object or prefix to the local disk (terminates)
nexus-transfers check-files     — Verify a local copy against a remote nexus reference (terminates)
nexus-transfers check-files-ssh — Verify a remote SSH copy against the local reference (terminates)
nexus-transfers check-files-s3  — Verify an S3 copy against the local reference (terminates)
```

Legacy standalone commands are preserved for backward compatibility.
