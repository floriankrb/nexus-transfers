"""Sanity-check S3 credentials against the configured bucket.

Performs a round-trip on a small temporary object: ``put`` → ``get`` →
``delete``.  Intended as a quick diagnostic before launching a real
transfer with ``--use-s3``.
"""

import argparse
import hashlib
import logging
import os
import sys
import uuid

import obstore as obs

from nexus_transfers import s3 as _s3
from nexus_transfers.config import resolve

logger = logging.getLogger(__name__)


_TEST_PAYLOAD = b"nexus-transfers credentials check\n"


def _settings_summary() -> dict[str, str]:
    """Return a redacted view of the resolved S3 settings."""
    raw_bucket = resolve(_s3.S3_BUCKET_ENV, default=None) or "(unset)"
    endpoint = resolve(_s3.S3_ENDPOINT_ENV, default=None) or "(default AWS)"
    access_key = resolve(_s3.S3_ACCESS_KEY_ENV, default=None)
    secret_key = resolve(_s3.S3_SECRET_KEY_ENV, default=None)
    return {
        "bucket": raw_bucket,
        "endpoint": endpoint,
        "access_key": _redact(access_key),
        "secret_key": "(set)" if secret_key else "(default AWS chain)",
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
    settings = _settings_summary()
    print("S3 configuration:")
    for key, value in settings.items():
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
    print(f"\nTest object: s3://{bucket}/{key}")

    try:
        store = _s3.get_store()
    except Exception as exc:
        print(f"\nFAIL: cannot build S3 store: {exc}", file=sys.stderr)
        if verbose:
            raise
        return 1

    expected = hashlib.sha256(_TEST_PAYLOAD).hexdigest()

    # --- PUT ---
    try:
        obs.put(store, key, _TEST_PAYLOAD)
    except Exception as exc:
        print(f"\nFAIL: upload (put) failed: {exc}", file=sys.stderr)
        if verbose:
            raise
        return 1
    print("  put     OK")

    # --- GET ---
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

    # --- DELETE ---
    try:
        obs.delete(store, key)
    except Exception as exc:
        print(f"\nFAIL: delete failed: {exc}", file=sys.stderr)
        if verbose:
            raise
        return 1
    print("  delete  OK")

    print(f"\nS3 credentials look good for s3://{bucket}/{prefix}/.")
    return 0


def _try_delete(store, key: str) -> None:
    """Best-effort delete used during error paths."""
    try:
        obs.delete(store, key)
    except Exception as exc:
        logger.warning("Could not clean up test object %s: %s", key, exc)


def main() -> None:
    """CLI entry point: ``nexus-transfers check``."""
    parser = argparse.ArgumentParser(
        prog="nexus-transfers check",
        description="Verify the configured S3 bucket is reachable "
                    "(put/get/delete a small test object).",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Show full tracebacks on failure.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    sys.exit(check_s3(verbose=args.verbose))


if __name__ == "__main__":
    main()
