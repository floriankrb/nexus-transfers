"""nexus_transfers.client – public API re-exports.

All symbols previously importable from ``nexus_transfers.client`` are
re-exported here so that downstream code continues to work unchanged.
"""

from ._client import Client, _DEFAULT_URL  # noqa: F401
from ._errors import (  # noqa: F401
    NameTakenError,
    PeerNotFoundError,
    RemoteError,
)
from ._interactive import main  # noqa: F401
from ._io import _write_file  # noqa: F401
