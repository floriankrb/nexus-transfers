"""Configuration loader for nexus-transfers.

Settings live in the ``[nexus]`` table of the anemoi config files,
``~/.config/anemoi/settings.toml`` and ``~/.config/anemoi/settings.secrets.toml``
(the latter wins; put credentials there, mode 0600). ``$ANEMOI_SETTINGS`` /
``$XDG_CONFIG_HOME`` move them, as for anemoi-nexus-client. The old
``~/.config/nexus-transfers/settings.toml`` and ``~/.nexus-transfers.toml``
are still read underneath them, with a deprecation warning. Helpers resolve
values with the precedence: CLI option > environment variable > config file >
default.

Broker and S3 settings live directly in ``[nexus]``, keyed by the lowercase
environment variable name without its prefix — ``NEXUS_TRANSFERS_*`` for the
broker connection, ``NEXUS_TRANSFERS_S3_*`` for S3 (see ``_ENV_MAP``). The old
singular ``NEXUS_TRANSFER_S3_*`` names are still read, with a deprecation
warning (see ``_DEPRECATED_ENV``).

Per-command CLI defaults are read from one sub-table per subcommand, the
key being the flag name without ``--`` and with dashes turned into
underscores: ``[nexus.broker]``, ``[nexus.client]`` (the ``server`` command),
``[nexus.monitor]``, ``[nexus.copy]``, ``[nexus.copy_ssh]``,
``[nexus.copy_s3]`` (both ``copy-to-s3`` and ``copy-from-s3``),
``[nexus.check_files]``, ``[nexus.check_files_ssh]`` and
``[nexus.check_files_s3]``.
"""

import logging
import os
import tomllib
from pathlib import Path

_SECTION = "nexus"


def _anemoi_settings() -> Path:
    """The anemoi settings file, located as anemoi-nexus-client does:
    ``$ANEMOI_SETTINGS``, else ``$XDG_CONFIG_HOME/anemoi/settings.toml``
    (``~/.config`` by default)."""
    explicit = os.environ.get("ANEMOI_SETTINGS")
    if explicit:
        return Path(explicit).expanduser()
    base = os.environ.get("XDG_CONFIG_HOME") or Path.home() / ".config"
    return Path(base) / "anemoi" / "settings.toml"


def _config_files() -> list[tuple[Path, bool]]:
    """``(path, deprecated)`` lowest priority first. The ``[nexus]`` table is
    taken from the anemoi files, the deprecated files are used whole."""
    settings = _anemoi_settings()
    return [
        (Path.home() / ".nexus-transfers.toml", True),
        (Path.home() / ".config" / "nexus-transfers" / "settings.toml", True),
        (settings, False),
        (settings.with_suffix(".secrets" + settings.suffix), False),
    ]


_config: dict | None = None
_logger = logging.getLogger(__name__)


def _merge(a: dict, b: dict) -> None:
    """Deep-merge *b* into *a* (``b`` wins)."""
    for k, v in b.items():
        if isinstance(v, dict) and isinstance(a.get(k), dict):
            _merge(a[k], v)
        else:
            a[k] = v


def _read(path: Path) -> dict:
    try:
        with open(path, "rb") as f:
            return tomllib.load(f)
    except PermissionError:
        _logger.warning("Cannot read config file %s (permission denied)", path)
    except tomllib.TOMLDecodeError as exc:
        _logger.warning("Invalid TOML in %s: %s", path, exc)
    return {}


def _load() -> dict:
    """Load, merge and cache the config files."""
    global _config
    if _config is not None:
        return _config
    _config = {}
    for path, deprecated in _config_files():
        if not path.exists():
            _logger.debug("Config file %s does not exist, skipping", path)
            continue
        data = _read(path)
        if deprecated:
            _logger.warning(
                "Reading config from deprecated %s — please move it to the [%s] "
                "table of %s", path, _SECTION, _anemoi_settings(),
            )
        else:
            data = data.get(_SECTION, {})
            if not isinstance(data, dict):
                _logger.warning("%s: [%s] is not a table, ignored", path, _SECTION)
                continue
        _merge(_config, data)
        _logger.debug("Loaded config from %s", path)
    return _config


def get(key: str, *, section: str | None = None, default=None):
    """Get a config value from the specified section (or ``[nexus]`` itself).

    Parameters
    ----------
    key : str
        Config key name (e.g. ``"url"``, ``"reconnect_retries"``).
    section : str or None
        Sub-table name under ``[nexus]`` (e.g. ``"broker"``, ``"client"``).
        If None, reads from ``[nexus]`` itself.
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
    "NEXUS_TRANSFERS_S3_BUCKET": "s3_bucket",
    "NEXUS_TRANSFERS_S3_ENDPOINT_URL": "s3_endpoint_url",
    "NEXUS_TRANSFERS_S3_ACCESS_KEY_ID": "s3_access_key_id",
    "NEXUS_TRANSFERS_S3_SECRET_ACCESS_KEY": "s3_secret_access_key",
    "NEXUS_TRANSFERS_S3_VIRTUAL_HOSTED_STYLE": "s3_virtual_hosted_style",
}

# Deprecated singular S3 prefix: new name -> old name, read when the new one
# is unset.
_DEPRECATED_ENV = {
    name: name.replace("NEXUS_TRANSFERS_S3_", "NEXUS_TRANSFER_S3_", 1)
    for name in _ENV_MAP if name.startswith("NEXUS_TRANSFERS_S3_")
}
_warned_env: set[str] = set()


def _getenv(env_var: str) -> str | None:
    """``os.environ.get`` that also honours the deprecated alias of *env_var*."""
    val = os.environ.get(env_var)
    if val is not None:
        return val
    old = _DEPRECATED_ENV.get(env_var)
    if old is None:
        return None
    val = os.environ.get(old)
    if val is not None and old not in _warned_env:
        _warned_env.add(old)
        _logger.warning("%s is deprecated — please rename it to %s", old, env_var)
    return val


def resolve(env_var: str, *, section: str | None = None, default=None):
    """Resolve a value with precedence: env var > config > default.

    Parameters
    ----------
    env_var : str
        Environment variable name (e.g. ``"NEXUS_TRANSFERS_URL"``).
    section : str or None
        Sub-table of ``[nexus]`` to look up the config key in.
    default :
        Fallback value if neither env nor config provides the value.

    Returns the env var value if set, otherwise the config value, otherwise
    the default.
    """
    val = _getenv(env_var)
    if val is not None:
        return val
    config_key = _ENV_MAP.get(env_var, env_var.lower())
    return get(config_key, section=section, default=default)


def resolve_bool(env_var: str, *, section: str | None = None, default: bool = False) -> bool:
    """Resolve a boolean value with precedence: env var > config > default."""
    env_val = _getenv(env_var)
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


BROKER_URL_ENV = "NEXUS_TRANSFERS_URL"


def broker_url_default(section: str) -> str | None:
    """Effective default for a command's ``--broker-url`` flag.

    Precedence (the CLI flag itself wins over all of it): ``$NEXUS_TRANSFERS_URL``
    > config ``[nexus.<section>] broker_url`` > config ``[nexus] url``.

    ``None`` means "no broker configured": the commands that need one fall back
    to ``client._DEFAULT_URL``, the ones that only use the broker for locking
    and monitoring (``copy-ssh``, ``copy-*-s3``, ``check-files-ssh``,
    ``check-files-s3``) run without it.
    """
    env_val = os.environ.get(BROKER_URL_ENV)
    if env_val:
        return env_val
    return get("broker_url", section=section, default=None) or get("url", default=None)


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
