"""Tests for the shared command-line parser (``_cli``)."""

from nexus_transfers import config
from nexus_transfers._cli import CommandParser


def _parser():
    parser = CommandParser("cmd")
    parser.option("--max-concurrent", type=int, default=4)
    parser.option("--hash", dest="algo", key="hash")
    parser.option("--fix", action="store_true")
    parser.monitor_options()
    parser.ssh_options()
    return parser


def test_defaults_without_config(monkeypatch):
    monkeypatch.setattr(config, "_config", {})
    monkeypatch.delenv("NEXUS_TRANSFERS_URL", raising=False)
    args = _parser().parse_args([])
    assert (args.max_concurrent, args.algo, args.fix) == (4, None, False)
    assert (args.broker_url, args.name, args.no_verify) == (None, None, False)
    assert (args.ssh_port, args.ssh_connections, args.cipher) == (22, 2, None)


def test_defaults_from_the_command_section(monkeypatch):
    monkeypatch.setattr(config, "_config", {
        "cmd": {"max_concurrent": "9", "hash": "sha1", "fix": True,
                "ssh_port": 2222, "broker_url": "ws://cfg"},
        "other": {"max_concurrent": 1},
    })
    monkeypatch.delenv("NEXUS_TRANSFERS_URL", raising=False)
    args = _parser().parse_args([])
    assert (args.max_concurrent, args.algo, args.fix) == (9, "sha1", True)
    assert (args.ssh_port, args.broker_url) == (2222, "ws://cfg")
    # the command line still wins
    assert _parser().parse_args(["--max-concurrent", "2"]).max_concurrent == 2
