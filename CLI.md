Refactor the nexus CLIs into a single one called "nexus-transfer" followed by a single word and then arguments

- broker (run the broker, runs forever)
- monitor (run a monitor, runs forever)
- server (run a peer awaiting for messages, runs forever)

Others will be instantiation of a peer that will initiate messages to servers, and terminate.
For example "nexus-transfer copy ..."

## Implemented

The unified CLI is in `src/nexus_transfers/cli.py`. Entry point: `nexus-transfer`.

```
nexus-transfer broker     — Run the WebSocket relay broker (runs forever)
nexus-transfer monitor    — Run a monitoring client (runs forever)
nexus-transfer server     — Run a peer awaiting messages (runs forever)
nexus-transfer copy       — Copy a remote directory to local via relay (terminates)
nexus-transfer copy-ssh   — Copy a local directory to a remote SSH target (terminates)
```

Legacy standalone commands are preserved for backward compatibility.
