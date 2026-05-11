"""Exception types raised by the nexus-transfers client."""


class RemoteError(Exception):
    """Raised when a remote function call returns an error."""

    def __init__(self, error, remote_traceback=None):
        super().__init__(error)
        self.remote_traceback = remote_traceback


class PeerNotFoundError(RemoteError):
    """Raised when the target peer is not registered on the relay."""


class NameTakenError(Exception):
    """Raised when another client already holds this name on the relay."""
