"""Nexus Transfers – WebSocket relay broker and RPC client."""

from nexus_transfers.copy import copy, list_dir
from nexus_transfers.client import (
    Client,
    NameTakenError,
    PeerNotFoundError,
    RemoteError,
)
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
    "copy",
    "list_dir",
    "adder",
    "echo",
    "Client",
    "NameTakenError",
    "PeerNotFoundError",
    "RemoteError",
    "DISPATCH",
    "FileTransfer",
    "S3Transfer",
    "make_get_file",
    "make_list_dir",
    "resolve_safe_path",
]
