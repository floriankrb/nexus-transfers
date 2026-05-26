"""asyncssh-backed SSH connection pool and SFTP helpers."""

import glob as _glob
import logging
import os
import uuid
from pathlib import PurePosixPath

import asyncssh

_logger = logging.getLogger(__name__)

# Hardware-accelerated ciphers preferred for bulk WAN transfers. AES-GCM uses
# AES-NI (2-3x the single-core throughput of aes*-ctr); chacha20-poly1305 is the
# fast fallback on hosts without AES-NI. Ordered by preference.
DEFAULT_ENCRYPTION_ALGS = [
    "aes128-gcm@openssh.com",
    "aes256-gcm@openssh.com",
    "chacha20-poly1305@openssh.com",
]


def _ssh_config_files(path: str = "~/.ssh/config") -> list[str]:
    """Expand ``Include`` directives in an OpenSSH config file.

    asyncssh does not follow ``Include`` directives itself, so this
    function reads the top-level config, collects every ``Include``
    target (resolved relative to ``~/.ssh/``), and returns a flat list
    of existing config file paths suitable for ``asyncssh.connect(config=...)``.
    """
    path = os.path.expanduser(path)
    if not os.path.isfile(path):
        return []
    ssh_dir = os.path.dirname(path)
    files = [path]
    with open(path) as fh:
        for line in fh:
            stripped = line.strip()
            if stripped.lower().startswith("include "):
                pattern = stripped.split(None, 1)[1]
                if not os.path.isabs(pattern):
                    pattern = os.path.join(ssh_dir, pattern)
                pattern = os.path.expanduser(pattern)
                files.extend(sorted(_glob.glob(pattern)))
    return [f for f in files if os.path.isfile(f)]


class SSHPool:
    """A pool of SSH connections, each with one SFTP client.

    Parameters
    ----------
    host : str
        Remote hostname.
    port : int
        SSH port.
    user : str or None
        Remote username; None uses the current OS user.
    key_path : str or None
        Path to the private key file; None uses the SSH agent or default keys.
    num_connections : int
        Number of SSH connections (and SFTP clients) to open.
    encryption_algs : list of str or None
        SSH cipher preference list passed to ``asyncssh.connect``; None uses
        :data:`DEFAULT_ENCRYPTION_ALGS` (hardware-accelerated GCM first).
    """

    def __init__(
        self,
        host: str,
        port: int,
        user: str | None,
        key_path: str | None,
        num_connections: int,
        encryption_algs: list[str] | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._user = user
        self._key_path = key_path
        self._num_connections = num_connections
        self._encryption_algs = encryption_algs or DEFAULT_ENCRYPTION_ALGS
        self._conns: list = []
        self._sftp_clients: list = []
        self._counter = 0

    async def connect(self) -> None:
        """Open SSH connections and start one SFTP client per connection."""
        for _ in range(self._num_connections):
            kwargs: dict = {
                "port": self._port,
                "known_hosts": None,
                "config": _ssh_config_files(),
                "encryption_algs": self._encryption_algs,
            }
            if self._user:
                kwargs["username"] = self._user
            if self._key_path:
                kwargs["client_keys"] = [self._key_path]
            conn = await asyncssh.connect(self._host, **kwargs)
            sftp = await conn.start_sftp_client()
            self._conns.append(conn)
            self._sftp_clients.append(sftp)
        _logger.debug(
            "SSH pool: %d connection(s) opened to %s:%d",
            self._num_connections, self._host, self._port,
        )

    def get_sftp(self):
        """Return an SFTP client from the pool using round-robin selection."""
        idx = self._counter % len(self._sftp_clients)
        self._counter += 1
        return self._sftp_clients[idx]

    async def close(self) -> None:
        """Close all SFTP clients and SSH connections."""
        for sftp in self._sftp_clients:
            sftp.exit()
        for conn in self._conns:
            conn.close()
            await conn.wait_closed()
        self._sftp_clients.clear()
        self._conns.clear()

    async def __aenter__(self) -> "SSHPool":
        await self.connect()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()


async def write_file(sftp, local_path: str, remote_path: str) -> None:
    """Upload *local_path* to *remote_path* via SFTP.

    Parameters
    ----------
    sftp :
        asyncssh SFTP client.
    local_path : str
        Local source file path.
    remote_path : str
        Remote destination file path (POSIX).
    """
    remote_dir = str(PurePosixPath(remote_path).parent)
    remote_name = PurePosixPath(remote_path).name
    tmp_path = f"{remote_dir}/{remote_name}.{uuid.uuid4().hex[:8]}.tmp"
    await sftp.makedirs(remote_dir, exist_ok=True)
    try:
        await sftp.put(local_path, tmp_path)
        await sftp.posix_rename(tmp_path, remote_path)
    except BaseException:
        try:
            await sftp.remove(tmp_path)
        except asyncssh.SFTPError:
            pass
        raise


async def stat_remote(sftp, remote_path: str) -> int | None:
    """Return the byte size of *remote_path*, or None if it does not exist.

    Parameters
    ----------
    sftp :
        asyncssh SFTP client.
    remote_path : str
        Remote file path to stat.
    """
    try:
        attrs = await sftp.stat(remote_path)
        return attrs.size
    except asyncssh.SFTPError:
        return None
