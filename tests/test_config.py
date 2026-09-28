"""Resolution order of the broker URL and of the per-command CLI defaults."""

import pytest

from nexus_transfers import config as _config
from nexus_transfers.config import BROKER_URL_ENV, broker_url_default, cli_default


@pytest.fixture()
def cfg(monkeypatch):
    """Install a TOML config with a top-level url and a section broker_url."""
    monkeypatch.setattr(_config, "_config", {
        "url": "ws://top-level:8766",
        "copy_ssh": {"broker_url": "ws://section:8766"},
        "monitor": {},
    })


def test_env_beats_config_file(cfg, monkeypatch):
    """CLI > env > config: the section must not shadow the environment."""
    monkeypatch.setenv(BROKER_URL_ENV, "ws://env:8766")
    assert broker_url_default("copy_ssh") == "ws://env:8766"


def test_section_beats_top_level(cfg, monkeypatch):
    monkeypatch.delenv(BROKER_URL_ENV, raising=False)
    assert broker_url_default("copy_ssh") == "ws://section:8766"


def test_top_level_url_used_without_a_section_key(cfg, monkeypatch):
    """A configured broker also enables the monitoring-only commands."""
    monkeypatch.delenv(BROKER_URL_ENV, raising=False)
    assert broker_url_default("monitor") == "ws://top-level:8766"
    assert broker_url_default("check_files_s3") == "ws://top-level:8766"


def test_none_when_nothing_is_configured(monkeypatch):
    monkeypatch.setattr(_config, "_config", {})
    monkeypatch.delenv(BROKER_URL_ENV, raising=False)
    assert broker_url_default("copy_ssh") is None


def test_empty_env_var_is_ignored(cfg, monkeypatch):
    monkeypatch.setenv(BROKER_URL_ENV, "")
    assert broker_url_default("copy_ssh") == "ws://section:8766"


def test_api_default_broker_url_follows_the_same_order(cfg, monkeypatch):
    from nexus_transfers import api

    monkeypatch.setenv(BROKER_URL_ENV, "ws://env:8766")
    assert api.default_broker_url() == "ws://env:8766"
    monkeypatch.delenv(BROKER_URL_ENV, raising=False)
    assert api.default_broker_url() == "ws://section:8766"


def test_cli_default_still_reads_only_its_own_section(cfg, monkeypatch):
    monkeypatch.setattr(_config, "_config", {"copy": {"max_concurrent": 8}})
    assert cli_default("max_concurrent", "copy", default=4, type_fn=int) == 8
    assert cli_default("max_concurrent", "copy_ssh", default=4, type_fn=int) == 4
