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

Client names are 1 to 255 bytes once UTF-8 encoded; the empty string is not a
name but the reserved sentinel meaning "the broker" (as a source: the frame
comes from the broker, as a target: the frame is addressed to it). The message
name is likewise 1 to 255 bytes and never empty. Both ``encode_frame`` and
``decode_frame`` enforce this and raise ``ValueError``.
"""

PROTOCOL_VERSION = 1

MAX_NAME_LEN = 255
"""Longest name (client or message) the one-byte length prefix can carry."""

ENCODINGS = ("J", "R")
"""Valid payload encodings: ``'J'`` = JSON, ``'R'`` = raw bytes."""

_MAX_PAYLOAD = 0xFFFFFFFF  # the size field is a 4-byte unsigned integer


def _encode_name(value: str, field: str, *, allow_empty: bool) -> bytes:
    """UTF-8 encode a name and check it fits the 1–255 byte rule."""
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a str, got {type(value).__name__}")
    encoded = value.encode()
    if not encoded and not allow_empty:
        raise ValueError(f"{field} must be 1 to {MAX_NAME_LEN} bytes, got an empty string")
    if len(encoded) > MAX_NAME_LEN:
        raise ValueError(
            f"{field} must be at most {MAX_NAME_LEN} bytes, "
            f"got {len(encoded)} ({value[:32]!r}…)"
        )
    return encoded


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
        Sender name, 1 to 255 bytes UTF-8. Use ``""`` for frames originated by
        the broker.
    msg_name:
        Message type string, 1 to 255 bytes UTF-8, e.g. ``"register"``,
        ``"call"``, ``"chunk"``.
    target:
        Destination client name, 1 to 255 bytes UTF-8. Use ``""`` to address
        the broker.
    encoding:
        ``'J'`` for a JSON payload, ``'R'`` for opaque bytes.
    payload:
        Raw payload bytes (already serialised if JSON).

    Raises
    ------
    ValueError
        If a name is empty where a name is required or longer than 255 bytes,
        if the encoding is not ``'J'`` or ``'R'``, or if the payload does not
        fit the 4-byte size field.
    """
    src_b = _encode_name(source, "source", allow_empty=True)
    msg_b = _encode_name(msg_name, "msg_name", allow_empty=False)
    tgt_b = _encode_name(target, "target", allow_empty=True)

    if encoding not in ENCODINGS:
        raise ValueError(f"encoding must be one of {ENCODINGS}, got {encoding!r}")
    if len(payload) > _MAX_PAYLOAD:
        raise ValueError(f"payload is {len(payload)} bytes, at most {_MAX_PAYLOAD} allowed")

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
        If the frame is truncated, has trailing bytes, contains invalid UTF-8,
        has an empty message name or an unknown payload encoding. The version
        is returned as-is: the broker answers an unsupported one with an
        ``error`` frame rather than dropping it.
    """
    offset = 0

    def take(n: int, field: str) -> bytes:
        nonlocal offset
        end = offset + n
        if end > len(raw):
            raise ValueError(
                f"malformed frame: truncated {field}, "
                f"need {n} bytes at offset {offset}, {len(raw) - offset} left"
            )
        chunk = raw[offset:end]
        offset = end
        return chunk

    def take_name(field: str, *, allow_empty: bool) -> str:
        length = take(1, f"{field} length")[0]
        if length == 0 and not allow_empty:
            raise ValueError(f"malformed frame: empty {field}")
        try:
            return take(length, field).decode()
        except UnicodeDecodeError as exc:
            raise ValueError(f"malformed frame: {field} is not valid UTF-8: {exc}") from exc

    version = take(1, "version")[0]
    source = take_name("source", allow_empty=True)
    msg_name = take_name("msg_name", allow_empty=False)
    target = take_name("target", allow_empty=True)

    encoding = chr(take(1, "encoding")[0])
    if encoding not in ENCODINGS:
        raise ValueError(f"malformed frame: unknown payload encoding {encoding!r}")

    size = int.from_bytes(take(4, "payload size"), "big")
    payload = take(size, "payload")
    if offset != len(raw):
        raise ValueError(f"malformed frame: {len(raw) - offset} trailing bytes after payload")

    return version, source, msg_name, target, encoding, payload
