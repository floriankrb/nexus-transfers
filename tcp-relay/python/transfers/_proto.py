"""
Binary framing for the transfers application protocol.

Frame layout (9-byte header + payload):
    [1]  message type  (uint8)
    [4]  payload length (big-endian uint32)
    [4]  CRC-32/IEEE of payload (big-endian uint32)
    [N]  payload bytes

This module mirrors transfers/client/proto.go exactly so that Go and Python
clients are wire-compatible.
"""
from __future__ import annotations

import json
import struct
import zlib
from dataclasses import dataclass, field
from typing import Any

# ── message types ─────────────────────────────────────────────────────────────

MSG_MANIFEST   = 0x01
MSG_RESUME     = 0x02
MSG_FILE_START = 0x03
MSG_CHUNK      = 0x04
MSG_FILE_DONE  = 0x05
MSG_DONE       = 0x06
MSG_ERROR      = 0x07

# Maximum bytes per CHUNK frame (must match Go chunkSize = 4 MiB).
CHUNK_SIZE = 4 << 20

_HEADER = struct.Struct(">BII")  # type(1) + payload_len(4) + crc32(4)


# ── frame I/O ─────────────────────────────────────────────────────────────────

def write_frame(sock, msg_type: int, payload: bytes) -> None:
    crc = zlib.crc32(payload) & 0xFFFF_FFFF
    header = _HEADER.pack(msg_type, len(payload), crc)
    sock.sendall(header + payload)


def read_frame(conn: "_Conn") -> tuple[int, bytes]:
    """Read one frame from conn, returning (type, payload).  Raises on CRC error."""
    header = conn.read_exact(_HEADER.size)
    msg_type, pay_len, want_crc = _HEADER.unpack(header)
    payload = conn.read_exact(pay_len) if pay_len else b""
    got_crc = zlib.crc32(payload) & 0xFFFF_FFFF
    if got_crc != want_crc:
        raise ValueError(
            f"CRC mismatch on frame type {msg_type:#04x}: "
            f"got {got_crc:#010x} want {want_crc:#010x}"
        )
    if msg_type == MSG_ERROR:
        raise RemoteError(payload.decode(errors="replace"))
    return msg_type, payload


def write_json(sock, msg_type: int, obj: Any) -> None:
    write_frame(sock, msg_type, json.dumps(obj, separators=(",", ":")).encode())


def read_expect_json(conn: "_Conn", want_type: int) -> Any:
    typ, payload = read_frame(conn)
    if typ != want_type:
        raise ProtocolError(f"expected frame {want_type:#04x}, got {typ:#04x}")
    return json.loads(payload)


def write_error(sock, message: str) -> None:
    try:
        write_frame(sock, MSG_ERROR, message.encode())
    except OSError:
        pass


# ── exceptions ────────────────────────────────────────────────────────────────

class RemoteError(Exception):
    """Error message received from the remote side."""


class ProtocolError(Exception):
    """Unexpected frame type or malformed message."""


# ── buffered reader ────────────────────────────────────────────────────────────

class _Conn:
    """
    Thin wrapper that provides:
      - read_exact(n)  — read exactly n bytes (blocks until available)
      - recv_line()    — read until newline, returning the line without \\n
      - send_line(s)   — send s + \\n
      - Underlying socket accessible as .sock

    Maintains an internal byte buffer so that the relay handshake (line-based)
    and the binary frame protocol can share the same connection without losing
    bytes.
    """

    def __init__(self, sock) -> None:
        self.sock = sock
        self._buf = bytearray()

    def _fill(self, need: int) -> None:
        while len(self._buf) < need:
            chunk = self.sock.recv(max(need - len(self._buf), 65536))
            if not chunk:
                raise EOFError("connection closed")
            self._buf += chunk

    def read_exact(self, n: int) -> bytes:
        if n == 0:
            return b""
        self._fill(n)
        out = bytes(self._buf[:n])
        del self._buf[:n]
        return out

    def recv_line(self) -> str:
        while b"\n" not in self._buf:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise EOFError("connection closed during handshake")
            self._buf += chunk
        idx = self._buf.index(b"\n")
        line = self._buf[:idx].decode()
        del self._buf[: idx + 1]
        return line

    def send_line(self, s: str) -> None:
        self.sock.sendall((s + "\n").encode())

    def send_all(self, data: bytes) -> None:
        self.sock.sendall(data)


# ── protocol structs (as plain dicts, matching JSON field names in Go) ────────

@dataclass
class FileEntry:
    index: int
    path: str
    size: int

    def to_dict(self) -> dict:
        return {"i": self.index, "p": self.path, "s": self.size}

    @staticmethod
    def from_dict(d: dict) -> "FileEntry":
        return FileEntry(index=d["i"], path=d["p"], size=d["s"])


@dataclass
class Manifest:
    files: list[FileEntry]
    total_bytes: int

    def to_dict(self) -> dict:
        return {"files": [f.to_dict() for f in self.files], "total_bytes": self.total_bytes}

    @staticmethod
    def from_dict(d: dict) -> "Manifest":
        return Manifest(
            files=[FileEntry.from_dict(f) for f in d["files"]],
            total_bytes=d["total_bytes"],
        )


@dataclass
class ResumeEntry:
    index: int
    offset: int

    def to_dict(self) -> dict:
        return {"i": self.index, "o": self.offset}

    @staticmethod
    def from_dict(d: dict) -> "ResumeEntry":
        return ResumeEntry(index=d["i"], offset=d["o"])


@dataclass
class Resume:
    files: list[ResumeEntry] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {"files": [f.to_dict() for f in self.files]}

    @staticmethod
    def from_dict(d: dict) -> "Resume":
        return Resume(files=[ResumeEntry.from_dict(f) for f in d.get("files", [])])


@dataclass
class FileStart:
    index: int
    path: str
    size: int
    offset: int

    def to_dict(self) -> dict:
        return {"i": self.index, "p": self.path, "s": self.size, "o": self.offset}

    @staticmethod
    def from_dict(d: dict) -> "FileStart":
        return FileStart(index=d["i"], path=d["p"], size=d["s"], offset=d["o"])


@dataclass
class FileDone:
    index: int
    sha256: str

    def to_dict(self) -> dict:
        return {"i": self.index, "sha256": self.sha256}

    @staticmethod
    def from_dict(d: dict) -> "FileDone":
        return FileDone(index=d["i"], sha256=d["sha256"])
