"""
transfers — relay-based file-transfer client.

Library usage::

    from transfers import copy, copy_many, serve, serve_loop

    # Pull a file or directory from a remote machine
    copy("relay.example.com", "machineB",
         src="machineA:/data/", dst="./data/")

    # Push to a remote machine
    copy("relay.example.com", "machineB",
         src="./data/", dst="machineA:/dest/data/")

    # Pull 4 items in parallel (remote side must `serve_loop(..., n=4)`)
    copy_many("relay.example.com", "machineB", [
        ("machineA:/data/file0.tar", "./file0.tar"),
        ("machineA:/data/file1.tar", "./file1.tar"),
    ])

    # On the remote side — serve 4 parallel slots
    serve_loop("relay.example.com", "machineA", n=4)

Spec format
-----------
- ``"peerName:/remote/path"``  — remote peer
- ``"./local/path"``           — local path (file or directory)
- ``"me:/local/path"``         — local path (explicit self-reference)

After the relay handshake both sides speak a binary application protocol
(MANIFEST → RESUME → chunks → SHA-256 verification).  Interrupted transfers
resume automatically from the last 4 MiB checkpoint.
"""

from __future__ import annotations

import base64
import hashlib
import os
import socket
import ssl
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Sequence

from ._proto import _Conn
from ._transfer import run_receiver, run_sender

__all__ = ["copy", "copy_many", "serve", "serve_loop"]

_WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def _ws_accept(key: str) -> str:
    return base64.b64encode(
        hashlib.sha1((key + _WS_GUID).encode()).digest()
    ).decode()


# ── relay handshake ───────────────────────────────────────────────────────────

def _connect(
    relay: str,
    name: str,
    *,
    port: int = 443,
    insecure: bool = False,
) -> _Conn:
    ctx = ssl.create_default_context()
    if insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    raw = socket.create_connection((relay, port))
    sock = ctx.wrap_socket(raw, server_hostname=relay)
    c = _Conn(sock)

    # HTTP/1.1 upgrade — looks like WebSocket to DPI; after 101 it's raw bytes,
    # no WebSocket framing and no XOR masking ever happens.
    key = base64.b64encode(os.urandom(16)).decode()
    request = (
        f"GET /relay HTTP/1.1\r\n"
        f"Host: {relay}\r\n"
        f"Upgrade: websocket\r\n"
        f"Connection: Upgrade\r\n"
        f"Sec-WebSocket-Version: 13\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        f"\r\n"
    )
    c.sock.sendall(request.encode())

    # Read HTTP response — headers use CRLF; strip both \r and \n.
    status = c.recv_line().strip()
    if "101" not in status:
        sock.close()
        raise RuntimeError(f"HTTP upgrade failed: {status!r}")
    want = _ws_accept(key)
    got = ""
    while True:
        line = c.recv_line().strip()  # strip \r left over from CRLF
        if not line:
            break
        if line.lower().startswith("sec-websocket-accept:"):
            got = line.split(":", 1)[1].strip()
    if got != want:
        sock.close()
        raise RuntimeError(f"Sec-WebSocket-Accept mismatch (got {got!r} want {want!r})")

    c.send_line(f"HELLO {name}")
    resp = c.recv_line()
    if resp != "OK":
        sock.close()
        raise RuntimeError(f"relay handshake rejected: {resp}")
    return c


def _parse_spec(spec: str) -> tuple[str, str]:
    """``"name:path"`` → ``(name, path)``.  ``"me:..."`` → ``("", ...)``. """
    if ":" not in spec:
        return "", spec
    name, _, path = spec.partition(":")
    return ("" if name == "me" else name), path


# ── public API ────────────────────────────────────────────────────────────────

def copy(
    relay: str,
    name: str,
    src: str,
    dst: str,
    *,
    port: int = 443,
    insecure: bool = False,
    progress=None,
) -> None:
    """Transfer a file or directory tree between this client and a remote peer.

    Exactly one of *src* / *dst* must name a remote peer (``"peer:/path"``);
    the other is a local path.

    *progress* may be a :class:`transfers._progress.Progress` instance for
    live terminal display.  Pass ``None`` (default) for no progress output.

    Interrupted transfers resume automatically.
    """
    c = _connect(relay, name, port=port, insecure=insecure)
    try:
        c.send_line(f"COPY {src} {dst}")
        resp = c.recv_line()
        if resp != "GO":
            raise RuntimeError(f"relay error: {resp}")

        src_name, src_path = _parse_spec(src)
        _, dst_path = _parse_spec(dst)

        if src_name:
            run_receiver(c, dst_path, progress)
        else:
            run_sender(c, src_path, progress)
    finally:
        try:
            c.sock.close()
        except OSError:
            pass


def serve(
    relay: str,
    name: str,
    *,
    port: int = 443,
    insecure: bool = False,
    progress=None,
) -> None:
    """Register as *name* and handle one incoming transfer.

    Blocks until the transfer completes.  Call again (or use
    :func:`serve_loop`) to accept further transfers.
    """
    c = _connect(relay, name, port=port, insecure=insecure)
    try:
        c.send_line("SERVE")
        cmd = c.recv_line()
        verb, _, path = cmd.partition(" ")
        path = path.strip()
        if verb == "SEND":
            run_sender(c, path, progress)
        elif verb == "RECV":
            run_receiver(c, path, progress)
        else:
            raise RuntimeError(f"unexpected relay command: {cmd!r}")
    finally:
        try:
            c.sock.close()
        except OSError:
            pass


def copy_many(
    relay: str,
    name: str,
    pairs: Sequence[tuple[str, str]],
    *,
    workers: int | None = None,
    port: int = 443,
    insecure: bool = False,
    progress=None,
) -> None:
    """Transfer multiple items concurrently.

    *pairs* is a sequence of ``(src, dst)`` tuples.  Each pair opens its own
    connection; all transfers run in parallel.

    *workers* caps the thread-pool size (default: one thread per pair).
    The remote side must open the same number of ``serve`` slots.
    """
    n = workers if workers is not None else len(pairs)
    with ThreadPoolExecutor(max_workers=n) as pool:
        futs = {
            pool.submit(copy, relay, name, src, dst,
                        port=port, insecure=insecure, progress=progress): i
            for i, (src, dst) in enumerate(pairs)
        }
        for fut in as_completed(futs):
            fut.result()  # re-raises any exception


def serve_loop(
    relay: str,
    name: str,
    n: int = 1,
    *,
    port: int = 443,
    insecure: bool = False,
    progress=None,
) -> None:
    """Open *n* concurrent serve slots under *name*.

    Blocks until all *n* transfers complete.  Use this to match a
    :func:`copy_many` call that requests the same number of parallel transfers.
    """
    with ThreadPoolExecutor(max_workers=n) as pool:
        futs = [
            pool.submit(serve, relay, name,
                        port=port, insecure=insecure, progress=progress)
            for _ in range(n)
        ]
        for f in futs:
            f.result()
