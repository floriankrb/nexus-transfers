"""Argument parsing shared by the ``nexus-transfers`` commands."""

import argparse
import asyncio
import sys

from nexus_transfers.config import broker_url_default, cli_default


class CommandParser(argparse.ArgumentParser):
    """An :class:`argparse.ArgumentParser` whose :meth:`option` defaults come
    from the config file section *section* (see
    :func:`~nexus_transfers.config.cli_default`), plus the option groups
    several commands share.

    Parameters
    ----------
    section : str
        Config file section of the command (e.g. ``"copy_ssh"``).
    **kwargs
        Forwarded to :class:`argparse.ArgumentParser`.
    """

    def __init__(self, section: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.section = section

    def option(self, *flags, default=None, type=None, key: str | None = None,
               **kwargs):
        """Add an option defaulting to the config value *key* (the option's
        dest by default), else *default*; ``store_true`` flags default to
        off."""
        if key is None:
            key = kwargs.get("dest") or flags[0].lstrip("-").replace("-", "_")
        if kwargs.get("action") == "store_true":
            default = cli_default(key, self.section, default=bool(default))
        else:
            default = cli_default(key, self.section, default=default,
                                  type_fn=type)
            kwargs["type"] = type
        return self.add_argument(*flags, default=default, **kwargs)

    def broker_options(self, help: str, *, name_help: str | None = None,
                       site_help: str | None = None,
                       no_verify_help: str) -> None:
        """``--broker-url``, ``--name`` (unless *name_help* is None),
        ``--site`` (unless *site_help* is None) and ``--no-verify``."""
        self.add_argument("--broker-url",
                          default=broker_url_default(self.section), help=help)
        if name_help is not None:
            self.option("--name", help=name_help)
        if site_help is not None:
            self.option("--site", help=site_help)
        self.option("--no-verify", action="store_true", help=no_verify_help)

    def monitor_options(self) -> None:
        """The broker options of the commands that use the broker only for
        monitoring (and locking): without one they run unmonitored."""
        self.broker_options(
            "Relay WebSocket URL for monitoring (default: "
            "$NEXUS_TRANSFERS_URL, else the section's broker_url / "
            "[nexus] url in the config file; none of them set — "
            "monitoring disabled)",
            name_help="Client name on the relay (default: auto-generated)",
            site_help="Site label for monitor messages",
            no_verify_help="Skip TLS verification for the relay connection",
        )

    def reconnect_options(self) -> None:
        """``--reconnect-retries`` / ``--reconnect-delay``."""
        self.option("--reconnect-retries", type=int, default=-1,
                    help="Reconnection attempts on disconnect "
                         "(-1 = infinite, default: -1)")
        self.option("--reconnect-delay", type=float, default=2.0,
                    help="Seconds between reconnection attempts (default: 2.0)")

    def peer_options(self) -> None:
        """``--peer-retries`` / ``--peer-delay`` / ``--call-timeout``."""
        self.option("--peer-retries", type=int, default=-1,
                    help="Retries when target peer is not found "
                         "(-1 = infinite, default: -1)")
        self.option("--peer-delay", type=float, default=2.0,
                    help="Seconds between peer-not-found retries (default: 2.0)")
        self.option("--call-timeout", type=float,
                    help="Timeout in seconds for RPC calls (default: no timeout)")

    def ssh_options(self, connections_help: str = "Number of SSH "
                    "connections to open (default: 2)") -> None:
        """``--ssh-port`` / ``--ssh-key`` / ``--ssh-connections`` /
        ``--cipher``."""
        self.option("--ssh-port", type=int, default=22,
                    help="SSH port (default: 22)")
        self.option("--ssh-key", help="Path to SSH private key")
        self.option("--ssh-connections", type=int, default=2,
                    help=connections_help)
        self.add_argument("--cipher", nargs="+", default=None, metavar="ALG",
                          help="SSH cipher preference list "
                               "(default: aes128-gcm@openssh.com first)")

    def debug_option(self) -> None:
        """``--debug``."""
        self.option("--debug", action="store_true", help="Enable debug logging")


def run_check(coro) -> None:
    """Run a ``check-files*`` coroutine and exit like the commands do: 2 when
    the check could not run, 1 when unfixed discrepancies remain."""
    from nexus_transfers.check_files import CheckFailedError

    try:
        report = asyncio.run(coro)
    except CheckFailedError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(2)
    if not report.ok:
        sys.exit(1)
