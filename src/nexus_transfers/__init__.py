"""Nexus Transfers – WebSocket relay server and RPC client."""

from nexus_transfers.client import Client, NameTakenError, PeerNotFoundError, RemoteError
from nexus_transfers.dispatch import (
    DISPATCH,
    FileTransfer,
    S3Transfer,
    adder,
    echo,
    make_get_file,
    make_list_dir,
    resolve_safe_path,
)

__all__ = [
    "Client",
    "DISPATCH",
    "FileTransfer",
    "NameTakenError",
    "PeerNotFoundError",
    "RemoteError",
    "S3Transfer",
    "adder",
    "echo",
    "make_get_file",
    "make_list_dir",
    "resolve_safe_path",
]
