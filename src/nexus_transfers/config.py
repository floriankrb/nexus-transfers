"""Configuration loader for nexus-transfers.

Reads ``~/.nexus-transfers.toml`` and provides helpers to resolve values
with the precedence: CLI option > environment variable > config file > default.

Environment variables starting with ``NEXUS_TRANSFER_`` (or ``NEXUS_TRANSFERS_``)
map to config keys in lowercase without the prefix.

Per-tool CLI defaults are read from TOML sections: ``[broker]``, ``[client]``,
``[copy]``, ``[monitor]``, ``[copy_ssh]``.
"""

import logging
import os
import tomllib
import warnings
from pathlib import Path

_CONFIG_PATH = Path.home() / ".config" / "nexus-transfers" / "settings.toml"
_LEGACY_CONFIG_PATH = Path.home() / ".nexus-transfers.toml"

_config: dict | None = None
_logger = logging.getLogger(__name__)


def _load() -> dict:
    """Load and cache the TOML config file."""
    global _config
    if _config is not None:
        return _config
    for path, legacy in [(_CONFIG_PATH, False), (_LEGACY_CONFIG_PATH, True)]:
        if not path.exists():
            _logger.debug("Config file %s does not exist, skipping", path)
            continue
        if legacy:
            _logger.warning(
                "Reading config from deprecated %s — please move it to %s",
                path, _CONFIG_PATH,
            )
        try:
            with open(path, "rb") as f:
                _config = tomllib.load(f)
            _logger.debug("Loaded config from %s", path)
        except PermissionError:
            _logger.warning("Cannot read config file %s (permission denied)", path)
            _config = {}
        except tomllib.TOMLDecodeError as exc:
            _logger.warning("Invalid TOML in %s: %s", path, exc)
            _config = {}
        return _config
    _config = {}
    return _config


def get(key: str, *, section: str | None = None, default=None):
    """Get a config value from the specified section (or top-level).

    Parameters
    ----------
    key : str
        Config key name (e.g. ``"url"``, ``"reconnect_retries"``).
    section : str or None
        TOML section name (e.g. ``"broker"``, ``"client"``).
        If None, reads from the top-level table.
    default :
        Fallback value if the key is not found.
    """
    cfg = _load()
    if section:
        table = cfg.get(section, {})
    else:
        table = cfg
    return table.get(key, default)


def get_section(section: str) -> dict:
    """Return a full section dict (empty dict if missing)."""
    cfg = _load()
    return dict(cfg.get(section, {}))


# ---------------------------------------------------------------------------
# Environment-variable to config-key mapping
# ---------------------------------------------------------------------------

# Maps env var name -> (config_key, section_or_None)
_ENV_MAP = {
    "NEXUS_TRANSFERS_URL": "url",
    "NEXUS_TRANSFERS_USER": "user",
    "NEXUS_TRANSFERS_PASSWORD": "password",
    "NEXUS_TRANSFER_S3_BUCKET": "s3_bucket",
    "NEXUS_TRANSFER_S3_ENDPOINT_URL": "s3_endpoint_url",
    "NEXUS_TRANSFER_S3_ACCESS_KEY_ID": "s3_access_key_id",
    "NEXUS_TRANSFER_S3_SECRET_ACCESS_KEY": "s3_secret_access_key",
    "NEXUS_TRANSFER_S3_VIRTUAL_HOSTED_STYLE": "s3_virtual_hosted_style",
}


def resolve(env_var: str, *, section: str | None = None, default=None):
    """Resolve a value with precedence: env var > config > default.

    Parameters
    ----------
    env_var : str
        Environment variable name (e.g. ``"NEXUS_TRANSFERS_URL"``).
    section : str or None
        TOML section to look up the config key in.
    default :
        Fallback value if neither env nor config provides the value.

    Returns the env var value if set, otherwise the config value, otherwise
    the default.
    """
    val = os.environ.get(env_var)
    if val is not None:
        return val
    config_key = _ENV_MAP.get(env_var, env_var.lower())
    return get(config_key, section=section, default=default)


def resolve_bool(env_var: str, *, section: str | None = None, default: bool = False) -> bool:
    """Resolve a boolean value with precedence: env var > config > default."""
    env_val = os.environ.get(env_var)
    if env_val is not None:
        return env_val.strip().lower() in ("1", "true", "yes", "on")
    config_key = _ENV_MAP.get(env_var, env_var.lower())
    cfg_val = get(config_key, section=section, default=None)
    if cfg_val is not None:
        if isinstance(cfg_val, bool):
            return cfg_val
        return str(cfg_val).strip().lower() in ("1", "true", "yes", "on")
    return default


def _cast(value, type_fn):
    """Cast a config value to the target type, handling None."""
    if value is None:
        return None
    return type_fn(value)


def cli_default(key: str, section: str, env_var: str | None = None,
                default=None, type_fn=None):
    """Compute the effective default for a CLI argument.

    Precedence: environment variable > config[section][key] > default.
    """
    if env_var:
        env_val = os.environ.get(env_var)
        if env_val is not None:
            if type_fn:
                return type_fn(env_val)
            return env_val
    val = get(key, section=section, default=None)
    if val is not None:
        if type_fn:
            return type_fn(val)
        return val
    return default
