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
```

Legacy standalone commands are preserved for backward compatibility.
