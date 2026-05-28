"""Diagnostic checks for nexus-transfers configuration.

Two independent sub-checks:

* ``--s3``        Round-trip a small object through the configured S3
                  bucket (put → get → delete).
* ``--site NAME`` Resolve the site config from the anemoi registry
                  (catalogue) and connect/register against its broker.

Both flags may be combined; at least one is required.
"""

import argparse
import asyncio
import hashlib
import logging
import sys
import uuid
from typing import Any

import obstore as obs

from nexus_transfers import s3 as _s3
from nexus_transfers.config import resolve

logger = logging.getLogger(__name__)


_TEST_PAYLOAD = b"nexus-transfers credentials check\n"


# ----------------------------------------------------------------------
# S3 round-trip
# ----------------------------------------------------------------------


def _settings_summary() -> dict[str, str]:
    """Return a redacted view of the resolved S3 settings."""
    return {
        "bucket": resolve(_s3.S3_BUCKET_ENV, default=None) or "(unset)",
        "endpoint": resolve(_s3.S3_ENDPOINT_ENV, default=None) or "(default AWS)",
        "access_key": _redact(resolve(_s3.S3_ACCESS_KEY_ENV, default=None)),
        "secret_key": (
            "(set)" if resolve(_s3.S3_SECRET_KEY_ENV, default=None)
            else "(default AWS chain)"
        ),
    }


def _redact(value: str | None) -> str:
    """Show only the first/last few chars of a secret-ish value."""
    if not value:
        return "(default AWS chain)"
    if len(value) <= 8:
        return "***"
    return f"{value[:4]}…{value[-2:]}"


def check_s3(verbose: bool = False) -> int:
    """Round-trip a tiny object on the configured bucket. Return 0 on success."""
    print("== S3 check ==", flush=True)
    for key, value in _settings_summary().items():
        print(f"  {key:11s} = {value}")

    if not _s3.is_configured():
        print(
            f"\nERROR: {_s3.S3_BUCKET_ENV} is not set — nothing to check.",
            file=sys.stderr,
        )
        return 2

    prefix = _s3._env_prefix() or "nexus-transfers"
    key = f"{prefix}/check/{uuid.uuid4()}.txt"
    bucket = _s3._env_bucket()
    print(f"  test object = s3://{bucket}/{key}")

    try:
        store = _s3.get_store()
    except Exception as exc:
        print(f"\nFAIL: cannot build S3 store: {exc}", file=sys.stderr)
        if verbose:
            raise
        return 1

    expected = hashlib.sha256(_TEST_PAYLOAD).hexdigest()

    try:
        obs.put(store, key, _TEST_PAYLOAD)
    except Exception as exc:
        print(f"\nFAIL: upload (put) failed: {exc}", file=sys.stderr)
        if verbose:
            raise
        return 1
    print("  put     OK")

    try:
        got = bytes(obs.get(store, key).bytes())
    except Exception as exc:
        print(f"\nFAIL: download (get) failed: {exc}", file=sys.stderr)
        _try_delete(store, key)
        if verbose:
            raise
        return 1
    actual = hashlib.sha256(got).hexdigest()
    if actual != expected:
        print(
            f"\nFAIL: checksum mismatch after round-trip "
            f"(expected {expected}, got {actual})",
            file=sys.stderr,
        )
        _try_delete(store, key)
        return 1
    print("  get     OK")

    try:
        obs.delete(store, key)
    except Exception as exc:
        print(f"\nFAIL: delete failed: {exc}", file=sys.stderr)
        if verbose:
            raise
        return 1
    print("  delete  OK")

    print(f"  S3 credentials look good for s3://{bucket}/{prefix}/.")
    return 0


def _try_delete(store, key: str) -> None:
    """Best-effort delete used during error paths."""
    try:
        obs.delete(store, key)
    except Exception as exc:
        logger.warning("Could not clean up test object %s: %s", key, exc)


# ----------------------------------------------------------------------
# Site + broker
# ----------------------------------------------------------------------


def _load_site(name: str) -> dict[str, Any]:
    """Resolve a site name to its catalogue config via ``anemoi.registry``."""
    try:
        from anemoi.registry import Site
    except ImportError as exc:
        raise RuntimeError(
            "anemoi-registry is not installed in this environment "
            "— cannot resolve site config from the catalogue"
        ) from exc
    return Site(name).data


async def _broker_round_trip(name: str, broker_url: str, ssl_verify: bool) -> None:
    """Connect to ``broker_url``, register a one-shot client, and disconnect."""
    from nexus_transfers import Client

    client_name = f"check-{name}-{uuid.uuid4().hex[:8]}"
    async with Client(client_name, url=broker_url, ssl_verify=ssl_verify):
        pass


def check_site(site_name: str, verbose: bool = False,
               ssl_verify: bool = True) -> int:
    """Resolve a site via the catalogue then dial its broker. Return 0 on success."""
    print(f"== Site check ({site_name}) ==", flush=True)

    try:
        data = _load_site(site_name)
    except Exception as exc:
        print(f"\nFAIL: cannot resolve site {site_name!r} from catalogue: "
              f"{exc}", file=sys.stderr)
        if verbose:
            raise
        return 1
    print("  catalogue OK")

    broker_url = data.get("broker_url")
    if not broker_url:
        print(f"\nFAIL: site {site_name!r} has no 'broker_url' in its catalogue "
              "config", file=sys.stderr)
        return 1
    print(f"  broker_url  = {broker_url}")

    user = resolve("NEXUS_TRANSFERS_USER")
    print(f"  user        = {user or '(unset)'}")

    try:
        asyncio.run(_broker_round_trip(site_name, broker_url, ssl_verify))
    except Exception as exc:
        print(f"\nFAIL: cannot connect/register to broker: {exc}",
              file=sys.stderr)
        if verbose:
            raise
        return 1
    print("  broker connect+register  OK")
    return 0


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------


def main() -> None:
    """CLI entry point: ``nexus-transfers check [--s3] [--site NAME]``."""
    parser = argparse.ArgumentParser(
        prog="nexus-transfers check",
        description="Diagnose nexus-transfers configuration. "
                    "At least one of --s3 or --site must be given.",
    )
    parser.add_argument(
        "--s3", action="store_true",
        help="Round-trip a small test object on the configured S3 bucket.",
    )
    parser.add_argument(
        "--site", metavar="NAME", default=None,
        help="Site name to resolve via the anemoi catalogue, then connect "
             "to its broker.",
    )
    parser.add_argument(
        "--no-ssl-verify", action="store_true",
        help="Skip TLS certificate verification when dialling the broker.",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Show full tracebacks on failure.",
    )
    args = parser.parse_args()

    if not args.s3 and not args.site:
        parser.error("nothing to check: pass --s3 and/or --site NAME")

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    rc = 0
    if args.s3:
        rc = max(rc, check_s3(verbose=args.verbose))
    if args.site:
        if args.s3:
            print()
        rc = max(rc, check_site(
            args.site, verbose=args.verbose,
            ssl_verify=not args.no_ssl_verify,
        ))
    sys.exit(rc)


if __name__ == "__main__":
    main()
