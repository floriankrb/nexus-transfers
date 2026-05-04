"""S3 staging helpers backed by ``obstore``.

When ``get_file`` is called with ``use_s3=True``, the source client (the one
holding the file) uploads it to a shared S3-compatible bucket and returns
the object key, size, SHA-256 checksum, **and the bucket name**. The
initiator then downloads from S3 using the bucket name received from the
source and asks the source to delete the staged object.

Configuration is read from the environment:

* ``NEXUS_TRANSFER_S3_BUCKET`` – bucket name (**required on the sending
  side**; the receiving side learns the bucket from the sender's reply).
* ``NEXUS_TRANSFER_S3_ENDPOINT_URL`` – endpoint URL (optional, for non-AWS
  S3-compatible services).
* ``NEXUS_TRANSFER_S3_ACCESS_KEY_ID`` – access key (optional, falls back to
  the standard AWS credential chain otherwise).
* ``NEXUS_TRANSFER_S3_SECRET_ACCESS_KEY`` – secret key (optional, ditto).
"""

import hashlib
import logging
import os
from pathlib import Path
from typing import Callable

import obstore as obs
from dotenv import load_dotenv

load_dotenv(Path.home() / ".env")

logger = logging.getLogger(__name__)

S3_BUCKET_ENV = "NEXUS_TRANSFER_S3_BUCKET"
S3_ENDPOINT_ENV = "NEXUS_TRANSFER_S3_ENDPOINT_URL"
S3_ACCESS_KEY_ENV = "NEXUS_TRANSFER_S3_ACCESS_KEY_ID"
S3_SECRET_KEY_ENV = "NEXUS_TRANSFER_S3_SECRET_ACCESS_KEY"
S3_VHOST_ENV = "NEXUS_TRANSFER_S3_VIRTUAL_HOSTED_STYLE"

_STREAM_CHUNK = 1024 * 1024


def is_configured() -> bool:
    """Return True if the bucket env var is set."""
    return bool(os.environ.get(S3_BUCKET_ENV))


def _split_bucket_spec(raw: str) -> tuple[str, str | None]:
    """Split a bucket spec into ``(bucket, prefix_or_None)``.

    Accepts the following forms:

    * ``my-bucket`` -> ``("my-bucket", None)``
    * ``s3://my-bucket`` -> ``("my-bucket", None)``
    * ``s3://my-bucket/sub/dir/`` -> ``("my-bucket", "sub/dir")``
    * ``my-bucket/sub`` -> ``("my-bucket", "sub")``
    """
    if raw.startswith("s3://"):
        raw = raw[len("s3://"):]
    raw = raw.strip("/")
    if "/" in raw:
        bucket, prefix = raw.split("/", 1)
        return bucket, prefix.strip("/") or None
    return raw, None


def _normalise_bucket(raw: str) -> str:
    """Return just the bucket portion of ``raw`` (no scheme, no prefix)."""
    return _split_bucket_spec(raw)[0]


def _env_bool(name: str, default: bool = False) -> bool:
    """Parse a boolean env var (``1/true/yes/on`` are truthy)."""
    val = os.environ.get(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


def _build_store_from_env(bucket_override: str | None = None):
    """Construct an ``S3Store`` from environment variables.

    Parameters
    ----------
    bucket_override
        If supplied, used as the bucket name and ``NEXUS_TRANSFER_S3_BUCKET``
        is not consulted.  Used by the receiving side, which learns the
        bucket from the sender's reply rather than from its own environment.
    """
    from obstore.store import S3Store

    if bucket_override is not None:
        bucket = bucket_override
    else:
        raw = os.environ.get(S3_BUCKET_ENV)
        if not raw:
            raise RuntimeError(
                f"{S3_BUCKET_ENV} is not set – S3 staging is not available"
            )
        bucket, _ = _split_bucket_spec(raw)
    kwargs: dict = {}
    endpoint = os.environ.get(S3_ENDPOINT_ENV)
    if endpoint:
        kwargs["endpoint"] = endpoint
        kwargs["virtual_hosted_style_request"] = _env_bool(S3_VHOST_ENV, False)
        kwargs["allow_http"] = endpoint.startswith("http://")
    access_key = os.environ.get(S3_ACCESS_KEY_ENV)
    if access_key:
        kwargs["access_key_id"] = access_key
    secret_key = os.environ.get(S3_SECRET_KEY_ENV)
    if secret_key:
        kwargs["secret_access_key"] = secret_key
    logger.debug(
        "S3 store: bucket=%r endpoint=%r virtual_hosted=%s",
        bucket, endpoint, kwargs.get("virtual_hosted_style_request"),
    )
    return S3Store(bucket=bucket, **kwargs)


# Patchable factory – tests swap this for a shared MemoryStore.
_store_factory: Callable[[], object] = _build_store_from_env


def get_store(bucket: str | None = None):
    """Return an object store. If ``bucket`` is given, override the env bucket.

    The override path is only taken when the factory is the default
    ``_build_store_from_env`` – tests that monkey-patch the factory always
    receive their substitute.
    """
    if bucket is None or _store_factory is not _build_store_from_env:
        return _store_factory()
    return _build_store_from_env(bucket_override=bucket)


def _env_prefix() -> str | None:
    """Extract the prefix portion of ``NEXUS_TRANSFER_S3_BUCKET`` if any."""
    raw = os.environ.get(S3_BUCKET_ENV)
    if not raw:
        return None
    return _split_bucket_spec(raw)[1]


def _env_bucket() -> str:
    """Return the bucket portion of ``NEXUS_TRANSFER_S3_BUCKET`` (or ``?``)."""
    raw = os.environ.get(S3_BUCKET_ENV)
    if not raw:
        return "?"
    return _split_bucket_spec(raw)[0]


def make_key(local_path: str) -> str:
    """Return the S3 key for ``local_path``.

    Uses the absolute source path stripped of its leading ``/``.  If the
    bucket env var includes a prefix (``s3://bucket/some/prefix``), the
    prefix is folded into the key so the receiving side only needs the
    bucket name to reach the object.
    """
    key = os.path.abspath(local_path).lstrip("/")
    prefix = _env_prefix()
    if prefix:
        return f"{prefix}/{key}"
    return key


def upload_file(
    local_path: str,
    progress_callback: Callable[[int], None] | None = None,
) -> tuple[str, str, int, str]:
    """Upload a local file and return ``(bucket, s3_key, size, sha256_hex)``.

    Streams the file in chunks while updating the SHA-256 hash so the
    file is read from disk only once.  The bucket name is included in the
    return value so the receiving client can reach the object without
    needing its own ``NEXUS_TRANSFER_S3_BUCKET``.

    Parameters
    ----------
    local_path
        Absolute path of the file to upload.
    progress_callback
        Optional callable invoked with the byte count of each chunk read.
    """
    store = get_store()
    bucket = _env_bucket()
    s3_key = make_key(local_path)
    size = os.path.getsize(local_path)
    hasher = hashlib.sha256()
    logger.debug("S3 upload: %s -> s3://%s/%s", local_path, bucket, s3_key)

    def _chunks():
        with open(local_path, "rb") as fh:
            while True:
                chunk = fh.read(_STREAM_CHUNK)
                if not chunk:
                    break
                hasher.update(chunk)
                if progress_callback is not None:
                    progress_callback(len(chunk))
                yield chunk

    obs.put(store, s3_key, _chunks())
    checksum = hasher.hexdigest()
    logger.debug("Upload of %s successful", local_path)
    return bucket, s3_key, size, checksum


def download_bytes(
    s3_key: str,
    expected_checksum: str | None = None,
    progress_callback: Callable[[int], None] | None = None,
    target: str | None = None,
    bucket: str | None = None,
) -> bytes:
    """Download an object from S3, verify its checksum, and return its bytes.

    Parameters
    ----------
    s3_key
        Object key in the configured bucket.
    expected_checksum
        Expected SHA-256 hex digest. ``ValueError`` is raised on mismatch.
    progress_callback
        Optional callable invoked with the byte count of each streamed chunk.
    target
        Optional human-readable destination, used only for log lines.
    bucket
        Bucket name returned by the sender.  When supplied, the receiver
        does not need ``NEXUS_TRANSFER_S3_BUCKET`` in its own environment.
    """
    store = get_store(bucket=bucket)
    bucket = bucket or _normalise_bucket(os.environ.get(S3_BUCKET_ENV, "?"))
    target_label = target or "(memory)"
    logger.debug("S3 download: s3://%s/%s -> %s", bucket, s3_key, target_label)
    result = obs.get(store, s3_key)
    pieces: list[bytes] = []
    hasher = hashlib.sha256()
    for chunk in result.stream():
        b = bytes(chunk)
        hasher.update(b)
        pieces.append(b)
        if progress_callback is not None:
            progress_callback(len(b))
    data = b"".join(pieces)
    actual = hasher.hexdigest()
    if expected_checksum is not None and actual != expected_checksum:
        raise ValueError(
            f"S3 download checksum mismatch: expected {expected_checksum}, "
            f"got {actual}"
        )
    logger.debug("Download of s3://%s/%s successful", bucket, s3_key)
    return data


def delete(s3_key: str) -> None:
    """Delete an object from the configured S3 bucket."""
    store = get_store()
    obs.delete(store, s3_key)
    logger.debug("Deleted s3://%s", s3_key)
