> **Status (2026-09-28):** implemented — broker pending-call tracking, error reply on peer death. Reference: [docs/PROTOCOL.md](../PROTOCOL.md).

The broker must keep track of messages that have not be replied to. If the peer handling the message dies before sending a reply, the broker will send an error reply to the originating peer.
