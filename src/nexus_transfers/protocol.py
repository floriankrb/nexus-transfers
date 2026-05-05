"""Binary frame codec for the nexus-transfers v1 protocol.

Frame layout
------------
  byte  field
  ----  -----
   1    version       uint8, must be 1
   1    src_len       uint8
   N    source        sender name (UTF-8); empty string for broker-originated frames
   1    msg_len       uint8
   M    msg_name      message type (UTF-8), e.g. "register", "call", "reply", "chunk"
   1    tgt_len       uint8; **0 means the frame is addressed to the broker itself**
   K    target        recipient client name (UTF-8); absent when tgt_len == 0
   1    encoding      ord('J') = JSON payload, ord('R') = raw bytes
   4    size          payload byte count, big-endian uint32
   P    payload       payload bytes

The broker only decodes the payload when the frame is addressed to itself (tgt_len == 0).
For all other frames it forwards the raw bytes without touching the payload.
"""

PROTOCOL_VERSION = 1


def encode_frame(
    source: str,
    msg_name: str,
    target: str,
    encoding: str,
    payload: bytes,
) -> bytes:
    """Encode a message into a binary protocol frame.

    Parameters
    ----------
    source:
        Sender name. Use ``""`` for frames originated by the broker.
    msg_name:
        Message type string, e.g. ``"register"``, ``"call"``, ``"chunk"``.
    target:
        Destination client name. Use ``""`` to address the broker.
    encoding:
        ``'J'`` for a JSON payload, ``'R'`` for opaque bytes.
    payload:
        Raw payload bytes (already serialised if JSON).
    """
    src_b = source.encode()
    msg_b = msg_name.encode()
    tgt_b = target.encode()
    header = bytes([
        PROTOCOL_VERSION,
        len(src_b), *src_b,
        len(msg_b), *msg_b,
        len(tgt_b), *tgt_b,
        ord(encoding),
    ])
    return header + len(payload).to_bytes(4, "big") + payload


def decode_frame(raw: bytes) -> tuple[int, str, str, str, str, bytes]:
    """Decode a binary protocol frame.

    Returns
    -------
    tuple
        ``(version, source, msg_name, target, encoding, payload)``

    Raises
    ------
    ValueError
        If the frame is too short or contains invalid UTF-8.
    """
    try:
        offset = 0
        version = raw[offset]; offset += 1

        src_len = raw[offset]; offset += 1
        source = raw[offset:offset + src_len].decode(); offset += src_len

        msg_len = raw[offset]; offset += 1
        msg_name = raw[offset:offset + msg_len].decode(); offset += msg_len

        tgt_len = raw[offset]; offset += 1
        target = raw[offset:offset + tgt_len].decode(); offset += tgt_len

        encoding = chr(raw[offset]); offset += 1

        size = int.from_bytes(raw[offset:offset + 4], "big"); offset += 4
        payload = raw[offset:offset + size]

        return version, source, msg_name, target, encoding, payload
    except (IndexError, UnicodeDecodeError) as exc:
        raise ValueError(f"malformed frame: {exc}") from exc
