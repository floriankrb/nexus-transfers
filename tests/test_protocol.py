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
    with pytest.raises(ValueError, match=r"source must be at most 255 bytes, got 256"):
        encode_frame("x" * 256, "call", "bob", "J", b"")
    with pytest.raises(ValueError, match=r"target must be at most 255 bytes"):
        encode_frame("alice", "call", "x" * 256, "J", b"")
    with pytest.raises(ValueError, match=r"msg_name must be at most 255 bytes"):
        encode_frame("alice", "x" * 256, "bob", "J", b"")


def test_encode_name_length_counts_utf8_bytes():
    """255 characters of 2-byte UTF-8 do not fit in a 255-byte name."""
    with pytest.raises(ValueError, match=r"got 510"):
        encode_frame("é" * 255, "call", "bob", "J", b"")
    frame = encode_frame("é" * 127, "call", "bob", "J", b"")
    _, source, *_ = decode_frame(frame)
    assert source == "é" * 127


def test_encode_rejects_empty_message_name():
    with pytest.raises(ValueError, match="msg_name must be 1 to 255 bytes"):
        encode_frame("alice", "", "bob", "J", b"{}")


def test_encode_rejects_unknown_encoding():
    with pytest.raises(ValueError, match="encoding must be one of"):
        encode_frame("alice", "call", "bob", "X", b"{}")


def test_decode_rejects_empty_message_name():
    frame = bytearray(encode_frame("alice", "call", "bob", "J", b"{}"))
    del frame[8:12]          # drop "call"
    frame[7] = 0             # msg_len = 0
    with pytest.raises(ValueError, match="empty msg_name"):
        decode_frame(bytes(frame))


def test_decode_rejects_unknown_encoding():
    frame = bytearray(encode_frame("alice", "call", "bob", "J", b"{}"))
    frame[frame.index(ord("J"), 16)] = ord("X")
    with pytest.raises(ValueError, match="unknown payload encoding"):
        decode_frame(bytes(frame))


def test_decode_rejects_truncated_payload():
    frame = encode_frame("alice", "call", "bob", "J", b"payload")
    with pytest.raises(ValueError, match="truncated payload"):
        decode_frame(frame[:-2])


def test_decode_rejects_trailing_bytes():
    frame = encode_frame("alice", "call", "bob", "J", b"{}")
    with pytest.raises(ValueError, match="trailing bytes"):
        decode_frame(frame + b"junk")


def test_decode_rejects_invalid_utf8_name():
    frame = bytearray(encode_frame("alice", "call", "bob", "J", b"{}"))
    frame[2] = 0xFF          # first byte of "alice"
    with pytest.raises(ValueError, match="source is not valid UTF-8"):
        decode_frame(bytes(frame))


def test_decode_keeps_unsupported_version():
    """The broker answers a bad version with an error frame, so decode must not raise."""
    frame = bytearray(encode_frame("alice", "call", "bob", "J", b"{}"))
    frame[0] = 99
    version, source, *_ = decode_frame(bytes(frame))
    assert (version, source) == (99, "alice")
