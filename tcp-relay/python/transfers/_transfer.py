"""
Sender and receiver logic for the transfers protocol.

Mirrors transfers/client/transfer.go exactly so that Go and Python clients
are fully wire-compatible.

Resume strategy
---------------
A hidden directory <dst_root>/.transfers/ holds one JSON state file per file
in the transfer manifest.  On crash/interruption the receiver:

  1. Reads the .tmp file size from disk.
  2. Truncates it to the last complete 4 MiB chunk boundary.
  3. Reports that offset in the RESUME message so the sender skips ahead.

The sender always computes a SHA-256 over the *entire* source file (including
the already-transferred prefix it does not resend).  After the last chunk the
sender sends FILE_DONE containing the full-file SHA-256; the receiver verifies
it before doing the atomic rename .tmp → final path.

Per-chunk CRC-32 in the frame header provides in-flight corruption detection.
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

from ._proto import (
    CHUNK_SIZE,
    MSG_CHUNK,
    MSG_DONE,
    MSG_ERROR,
    MSG_FILE_DONE,
    MSG_FILE_START,
    MSG_MANIFEST,
    MSG_RESUME,
    FileDone,
    FileEntry,
    FileStart,
    Manifest,
    Resume,
    ResumeEntry,
    _Conn,
    ProtocolError,
    RemoteError,
    read_expect_json,
    read_frame,
    write_error,
    write_frame,
    write_json,
)

if TYPE_CHECKING:
    from ._progress import Progress

STATE_DIR = ".transfers"


# ── sender ───────────────────────────────────────────────────────────────────

def run_sender(conn: _Conn, src_root: str, progress: "Progress | None" = None) -> None:
    """
    Walk src_root (file or directory), send the manifest, apply the resume
    reply, then stream all file data to conn.
    """
    entries = _build_manifest(src_root)
    total_bytes = sum(e.size for e in entries)
    manifest = Manifest(files=entries, total_bytes=total_bytes)
    write_json(conn.sock, MSG_MANIFEST, manifest.to_dict())

    # Read resume reply.
    raw = read_expect_json(conn, MSG_RESUME)
    resume = Resume.from_dict(raw)
    offsets = {re.index: re.offset for re in resume.files}

    if progress:
        progress.set_manifest(len(entries), total_bytes)
        for re in resume.files:
            progress.add_bytes(re.offset)

    for entry in entries:
        offset = offsets.get(entry.index, 0)
        # For a single-file transfer the src_root IS the file.
        abs_path = os.path.join(src_root, entry.path)
        if not os.path.exists(abs_path):
            abs_path = src_root  # single file

        if progress:
            progress.start_file(entry.path, entry.size, offset)

        sha256hex = _send_file(conn, abs_path, entry, offset, progress)

        write_json(conn.sock, MSG_FILE_DONE, FileDone(index=entry.index, sha256=sha256hex).to_dict())

        if progress:
            progress.finish_file()

    write_frame(conn.sock, MSG_DONE, b"")

    # Wait for receiver's DONE ack.
    typ, _ = read_frame(conn)
    if typ != MSG_DONE:
        raise ProtocolError(f"expected DONE ack, got frame type {typ:#04x}")


def _build_manifest(root: str) -> list[FileEntry]:
    root_path = Path(root)
    if not root_path.is_dir():
        return [FileEntry(index=0, path=root_path.name, size=root_path.stat().st_size)]

    entries: list[FileEntry] = []
    idx = 0
    for dirpath, _dirs, filenames in os.walk(root):
        for fname in sorted(filenames):
            full = os.path.join(dirpath, fname)
            rel = os.path.relpath(full, root)
            # Normalise to forward slashes for cross-platform wire format.
            rel_posix = PurePosixPath(rel).as_posix()
            size = os.path.getsize(full)
            entries.append(FileEntry(index=idx, path=rel_posix, size=size))
            idx += 1
    return entries


def _send_file(
    conn: _Conn,
    abs_path: str,
    entry: FileEntry,
    offset: int,
    progress: "Progress | None",
) -> str:
    hdr = FileStart(index=entry.index, path=entry.path, size=entry.size, offset=offset)
    write_json(conn.sock, MSG_FILE_START, hdr.to_dict())

    h = hashlib.sha256()
    with open(abs_path, "rb") as f:
        # Hash the already-transferred prefix without sending it.
        remaining = offset
        while remaining > 0:
            n = min(remaining, CHUNK_SIZE)
            chunk = f.read(n)
            if not chunk:
                break
            h.update(chunk)
            remaining -= len(chunk)

        # Send the rest in chunks.
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
            write_frame(conn.sock, MSG_CHUNK, chunk)
            if progress:
                progress.add_bytes(len(chunk))

    return h.hexdigest()


# ── receiver ─────────────────────────────────────────────────────────────────

def run_receiver(conn: _Conn, dst_root: str, progress: "Progress | None" = None) -> None:
    """Read the manifest, send resume info, then receive all files into dst_root."""
    raw = read_expect_json(conn, MSG_MANIFEST)
    manifest = Manifest.from_dict(raw)

    state_root = os.path.join(dst_root, STATE_DIR)
    os.makedirs(state_root, exist_ok=True)

    resume = _build_resume(dst_root, manifest.files)
    write_json(conn.sock, MSG_RESUME, resume.to_dict())

    if progress:
        progress.set_manifest(len(manifest.files), manifest.total_bytes)
        for re in resume.files:
            progress.add_bytes(re.offset)

    while True:
        typ, payload = read_frame(conn)

        if typ == MSG_FILE_START:
            fs = FileStart.from_dict(json.loads(payload))
            if progress:
                progress.start_file(fs.path, fs.size, fs.offset)
            _receive_file(conn, dst_root, fs, progress)
            if progress:
                progress.finish_file()

        elif typ == MSG_DONE:
            write_frame(conn.sock, MSG_DONE, b"")  # ack
            return

        elif typ == MSG_ERROR:
            raise RemoteError(payload.decode(errors="replace"))

        else:
            raise ProtocolError(f"unexpected frame type {typ:#04x}")


def _receive_file(
    conn: _Conn,
    dst_root: str,
    fs: FileStart,
    progress: "Progress | None",
) -> None:
    safe_rel = _sanitize_path(fs.path)
    dst_path = os.path.join(dst_root, safe_rel)
    tmp_path = dst_path + ".tmp"
    state_path = _state_file_path(dst_root, fs.index)

    os.makedirs(os.path.dirname(dst_path), exist_ok=True)

    h = hashlib.sha256()

    # Open the temp file: append if resuming, truncate otherwise.
    if fs.offset > 0:
        # Seed hasher with already-written data.
        try:
            with open(tmp_path, "rb") as existing:
                remaining = fs.offset
                while remaining > 0:
                    chunk = existing.read(min(remaining, CHUNK_SIZE))
                    if not chunk:
                        raise IOError("partial file shorter than expected")
                    h.update(chunk)
                    remaining -= len(chunk)
            mode = "ab"
        except (IOError, OSError):
            # Can't seed hash; restart from scratch.
            fs = FileStart(index=fs.index, path=fs.path, size=fs.size, offset=0)
            mode = "wb"
            h = hashlib.sha256()
    else:
        mode = "wb"

    _save_state(state_path, {
        "path": fs.path, "size": fs.size,
        "received": fs.offset, "done": False, "sha256": "",
    })

    written = fs.offset
    checkpoint = written

    with open(tmp_path, mode) as f:
        while True:
            typ, payload = read_frame(conn)

            if typ == MSG_CHUNK:
                f.write(payload)
                h.update(payload)
                written += len(payload)
                if progress:
                    progress.add_bytes(len(payload))
                # Persist checkpoint every 64 MiB.
                if written - checkpoint >= 64 << 20:
                    checkpoint = written
                    _save_state(state_path, {
                        "path": fs.path, "size": fs.size,
                        "received": written, "done": False, "sha256": "",
                    })

            elif typ == MSG_FILE_DONE:
                fd = FileDone.from_dict(json.loads(payload))
                got = h.hexdigest()
                if got != fd.sha256:
                    os.remove(tmp_path)
                    os.remove(state_path)
                    raise ValueError(
                        f"SHA-256 mismatch for {fs.path}: "
                        f"got {got[:8]}… want {fd.sha256[:8]}…"
                    )
                f.flush()
                break  # exit context manager, then rename

            elif typ == MSG_ERROR:
                raise RemoteError(payload.decode(errors="replace"))

            else:
                raise ProtocolError(f"unexpected frame {typ:#04x} in chunk stream")

    # Atomic rename.
    os.replace(tmp_path, dst_path)
    _save_state(state_path, {
        "path": fs.path, "size": fs.size,
        "received": fs.size, "done": True, "sha256": fd.sha256,  # type: ignore[name-defined]
    })


# ── resume state ─────────────────────────────────────────────────────────────

def _state_file_path(dst_root: str, index: int) -> str:
    return os.path.join(dst_root, STATE_DIR, f"{index}.state")


def _save_state(path: str, record: dict) -> None:
    try:
        with open(path, "w") as f:
            json.dump(record, f, indent=2)
    except OSError:
        pass


def _load_state(path: str) -> dict | None:
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _build_resume(dst_root: str, files: list[FileEntry]) -> Resume:
    entries: list[ResumeEntry] = []
    for fe in files:
        sp = _state_file_path(dst_root, fe.index)
        s = _load_state(sp)
        if s is None:
            continue
        if s.get("done"):
            dst_path = os.path.join(dst_root, _sanitize_path(fe.path))
            try:
                if os.path.getsize(dst_path) == fe.size:
                    entries.append(ResumeEntry(index=fe.index, offset=fe.size))
                    continue
            except OSError:
                pass
            os.remove(sp)
            continue
        # Partial: use last confirmed chunk boundary.
        tmp_path = os.path.join(dst_root, _sanitize_path(fe.path)) + ".tmp"
        try:
            actual = os.path.getsize(tmp_path)
        except OSError:
            continue
        confirmed = _align_to_chunk(actual)
        if confirmed <= 0:
            continue
        if actual > confirmed:
            _truncate(tmp_path, confirmed)
        entries.append(ResumeEntry(index=fe.index, offset=confirmed))
    return Resume(files=entries)


def _align_to_chunk(size: int) -> int:
    if size <= 0:
        return 0
    return (size // CHUNK_SIZE) * CHUNK_SIZE


def _truncate(path: str, size: int) -> None:
    try:
        with open(path, "ab") as f:
            f.truncate(size)
    except OSError:
        pass


def _sanitize_path(rel: str) -> str:
    """Convert a POSIX relative path from wire format to a local OS path,
    rejecting any path that would escape the destination root."""
    parts = PurePosixPath(rel).parts
    safe_parts = []
    for part in parts:
        if part in ("", ".", ".."):
            if part == "..":
                raise ValueError(f"path traversal attempt: {rel!r}")
        else:
            safe_parts.append(part)
    return os.path.join(*safe_parts) if safe_parts else "."
