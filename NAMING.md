# Naming Analysis

The current architecture has:

- **server** — a WebSocket relay that routes frames between connected clients
- **client** — any connected peer (worker nodes, copy tools, monitor)
- **source** / **target** — used in copy operations for the data origin and destination

## Recommendation

| Current term | Suggested term | Rationale |
|---|---|---|
| server | **broker** | It doesn't serve data — it routes messages between peers. "Broker" (as in message broker) precisely describes this role. |
| client | **peer** | All connected nodes are equals from the broker's perspective. They send and receive RPC calls to/from each other. "Peer" removes the asymmetry implied by "client/server". |
| source | **source** | Keep as-is. In a copy operation, "source" is clear and unambiguous for the data origin. |
| target (copy destination) | **destination** or **sink** | "Target" is overloaded — it's also used in the protocol for the peer a message is addressed to. Using **destination** (or **dest**) for the copy endpoint avoids confusion with the frame-level "target" field. |
| target (frame routing) | **recipient** | In the wire protocol, the peer a frame is addressed to could be called "recipient" to distinguish from the copy destination. However this is a deeper change. |

## Summary

The most impactful rename would be:

1. `server` → `broker` — reflects the relay/routing role
2. `client` → `peer` — reflects the symmetric relationship
3. Keep `source` for data origin in transfers
4. Consider `destination` instead of `target` for the copy endpoint to avoid collision with the protocol-level "target" field

The CLI names (`nexus-server`, `nexus-client`) could stay as-is for user familiarity, with the internal terminology shifting to broker/peer in code and documentation.
