"""Transfer – WebSocket relay server and RPC client."""

from transfer.client import Client
from transfer.dispatch import DISPATCH, adder, echo

__all__ = ["Client", "DISPATCH", "adder", "echo"]
