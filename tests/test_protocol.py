"""Round-trip and edge-case tests for the binary frame codec."""

import pytest

from nexus_transfers.protocol import PROTOCOL_VERSION, decode_frame, encode_frame


def test_round_trip():
    frame = encode_frame("alice", "call", "bob", "J", b'{"x": 1}')
    version, source, msg_name, target, encoding, payload = decode_frame(frame)
    assert version == PROTOCOL_VERSION
    assert source == "alice"
    assert msg_name == "call"
    assert target == "bob"
    assert encoding == "J"
    assert payload == b'{"x": 1}'


def test_round_trip_broker_frame():
    """Empty source/target address the broker."""
    frame = encode_frame("", "register", "", "J", b"{}")
    _, source, msg_name, target, _, payload = decode_frame(frame)
    assert source == ""
    assert target == ""
    assert msg_name == "register"
    assert payload == b"{}"


def test_round_trip_raw_encoding_empty_payload():
    frame = encode_frame("a", "chunk", "b", "R", b"")
    *_, encoding, payload = decode_frame(frame)
    assert encoding == "R"
    assert payload == b""


def test_round_trip_utf8_names():
    frame = encode_frame("élise", "call", "bob", "J", b"{}")
    _, source, _, target, _, _ = decode_frame(frame)
    assert source == "élise"
    assert target == "bob"


def test_decode_truncated_header_raises():
    frame = encode_frame("alice", "call", "bob", "J", b"payload")
    with pytest.raises(ValueError, match="malformed frame"):
        decode_frame(frame[:3])


def test_encode_rejects_oversize_name():
    """Name length is a single byte; longer names cannot be encoded."""
    with pytest.raises(ValueError):
        encode_frame("x" * 256, "call", "bob", "J", b"")
