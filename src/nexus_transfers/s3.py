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
import time
import uuid
from pathlib import Path
from typing import Callable

import obstore as obs
from dotenv import load_dotenv

from nexus_transfers.config import resolve, resolve_bool

load_dotenv(Path.home() / ".env")

logger = logging.getLogger(__name__)

S3_BUCKET_ENV = "NEXUS_TRANSFER_S3_BUCKET"
S3_ENDPOINT_ENV = "NEXUS_TRANSFER_S3_ENDPOINT_URL"
S3_ACCESS_KEY_ENV = "NEXUS_TRANSFER_S3_ACCESS_KEY_ID"
S3_SECRET_KEY_ENV = "NEXUS_TRANSFER_S3_SECRET_ACCESS_KEY"
S3_VHOST_ENV = "NEXUS_TRANSFER_S3_VIRTUAL_HOSTED_STYLE"

_STREAM_CHUNK = 1024 * 1024
_MAX_RETRIES = 3
_RETRY_BASE_DELAY = 1.0  # seconds; doubled each attempt

# Sentinel returned by retried callables to signal "no value" (a missing object
# or an exhausted listing) without that being mistaken for a transient failure
# worth retrying.
_NO_VALUE = object()

# Downloaded files always get 644, regardless of the process umask, so
# transferred datasets end up world-readable on every site.
_FILE_MODE = 0o644


def _with_retries(operation: Callable[[], object], what: str) -> object:
    """Run *operation*, retrying transient failures with exponential backoff.

    Mirrors the retry policy of :func:`upload_file`/:func:`download_file`: up to
    ``_MAX_RETRIES`` attempts with a ``_RETRY_BASE_DELAY``-second base delay,
    doubled each attempt. The last failure is re-raised. *operation* must be a
    zero-argument callable performing a single attempt; it should map any
    definitive non-transient outcome (e.g. "not found") onto a return value
    rather than an exception so it is not retried.

    Parameters
    ----------
    operation
        Zero-argument callable performing one attempt.
    what
        Short human description of the operation for retry log messages.
    """
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            return operation()
        except Exception as exc:
            if attempt == _MAX_RETRIES:
                raise
            delay = _RETRY_BASE_DELAY * 2 ** (attempt - 1)
            logger.warning(
                "S3 %s attempt %d/%d failed (%s), retrying in %.1fs …",
                what, attempt, _MAX_RETRIES, exc, delay,
            )
            time.sleep(delay)
    raise AssertionError("unreachable")  # pragma: no cover


def is_configured() -> bool:
    """Return True if the bucket env var or config is set."""
    return bool(resolve(S3_BUCKET_ENV, default=None))


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
    """Parse a boolean env var (``1/true/yes/on`` are truthy), with config fallback."""
    return resolve_bool(name, default=default)


def _build_store_from_env(bucket_override: str | None = None):
    """Construct an ``S3Store`` from environment variables / config.

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
        raw = resolve(S3_BUCKET_ENV, default=None)
        if not raw:
            raise RuntimeError(
                f"{S3_BUCKET_ENV} is not set – S3 staging is not available"
            )
        bucket, _ = _split_bucket_spec(raw)
    kwargs: dict = {}
    endpoint = resolve(S3_ENDPOINT_ENV, default=None)
    if endpoint:
        kwargs["endpoint"] = endpoint
        kwargs["virtual_hosted_style_request"] = _env_bool(S3_VHOST_ENV, False)
        kwargs["allow_http"] = endpoint.startswith("http://")
    access_key = resolve(S3_ACCESS_KEY_ENV, default=None)
    if access_key:
        kwargs["access_key_id"] = access_key
    secret_key = resolve(S3_SECRET_KEY_ENV, default=None)
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
    raw = resolve(S3_BUCKET_ENV, default=None)
    if not raw:
        return None
    return _split_bucket_spec(raw)[1]


def _env_bucket() -> str:
    """Return the bucket portion of ``NEXUS_TRANSFER_S3_BUCKET`` (or ``?``)."""
    raw = resolve(S3_BUCKET_ENV, default=None)
    if not raw:
        return "?"
    return _split_bucket_spec(raw)[0]


def parse_s3_url(raw: str) -> tuple[str, str | None]:
    """Parse an S3 URL into ``(bucket, prefix_or_None)``.

    Accepts ``s3://bucket``, ``s3://bucket/prefix`` and bare
    ``bucket[/prefix]`` forms.

    Parameters
    ----------
    raw
        S3 URL or bucket spec.

    Raises
    ------
    ValueError
        If no bucket name can be extracted.
    """
    bucket, prefix = _split_bucket_spec(raw)
    if not bucket:
        raise ValueError(
            f"Invalid S3 URL {raw!r}: expected s3://bucket[/prefix]"
        )
    return bucket, prefix


def list_objects(bucket: str, prefix: str | None = None) -> list[tuple[str, int]]:
    """List objects under *prefix* and return ``(key, size)`` pairs.

    Parameters
    ----------
    bucket
        Bucket name (plain name or ``s3://bucket`` URI).
    prefix
        Key prefix to list under; None lists the whole bucket.
    """
    store = get_store(bucket=bucket)
    out: list[tuple[str, int]] = []
    for batch in obs.list(store, prefix=prefix):
        for meta in batch:
            out.append((meta["path"], meta["size"]))
    return out


def list_object_batches(bucket: str, prefix: str | None = None):
    """Return a lazy iterator of object-metadata batches under *prefix*.

    Each yielded batch is a list of obstore metadata mappings (with ``path``
    and ``size`` keys). Unlike :func:`list_objects`, this does not buffer the
    whole listing, so callers can stream a very large bucket without holding
    every key in memory at once.

    Parameters
    ----------
    bucket
        Bucket name (plain name or ``s3://bucket`` URI).
    prefix
        Key prefix to list under; None lists the whole bucket.
    """
    store = get_store(bucket=bucket)
    return iter(obs.list(store, prefix=prefix))


def next_batch(iterator) -> list | None:
    """Pull the next listing page from *iterator*, retrying transient failures.

    Returns the next batch (a list of obstore metadata mappings), or ``None``
    once the listing is exhausted. A transient failure of the underlying page
    request is retried with exponential backoff (obstore keeps its continuation
    token, so the retried ``next`` resumes from the same page); a definitive
    ``StopIteration`` ends the listing without retrying.

    Parameters
    ----------
    iterator
        A listing iterator as returned by :func:`list_object_batches`.
    """
    def _once() -> object:
        try:
            return next(iterator)
        except StopIteration:
            return _NO_VALUE

    result = _with_retries(_once, "list page")
    return None if result is _NO_VALUE else result


def head_object(bucket: str, key: str) -> int | None:
    """Return the size of the object at *key*, or None if it does not exist.

    Transient failures of the ``HEAD`` request are retried with exponential
    backoff; a genuine "not found" is returned as ``None`` without retrying.

    Parameters
    ----------
    bucket
        Bucket name (plain name or ``s3://bucket`` URI).
    key
        Object key to stat.
    """
    store = get_store(bucket=bucket)

    def _once() -> object:
        try:
            return obs.head(store, key)["size"]
        except FileNotFoundError:
            return _NO_VALUE

    result = _with_retries(_once, f"head {key}")
    return None if result is _NO_VALUE else result


def make_key(local_path: str, s3_prefix: str | None = None) -> str:
    """Return the S3 key for ``local_path``.

    Uses the absolute source path stripped of its leading ``/``.  If
    ``s3_prefix`` is given it is prepended to the key.  Otherwise, if the
    bucket env var includes a prefix (``s3://bucket/some/prefix``), that
    prefix is used instead.

    Parameters
    ----------
    local_path
        File path to derive the key from.
    s3_prefix
        Optional prefix prepended to the key (e.g. a per-transfer
        directory name).
    """
    key = os.path.abspath(local_path).lstrip("/")
    if s3_prefix:
        return f"{s3_prefix}/{key}"
    prefix = _env_prefix()
    if prefix:
        return f"{prefix}/{key}"
    return key


def upload_file(
    local_path: str,
    progress_callback: Callable[[int], None] | None = None,
    s3_prefix: str | None = None,
    *,
    s3_key: str | None = None,
    bucket: str | None = None,
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
    s3_prefix
        Optional prefix prepended to the S3 key.
    s3_key
        Explicit object key; when given, ``make_key``/``s3_prefix`` are
        not used.
    bucket
        Bucket name (plain name or ``s3://bucket`` URI); when given it
        overrides ``NEXUS_TRANSFER_S3_BUCKET``.
    """
    store = get_store(bucket=bucket)
    bucket = _normalise_bucket(bucket) if bucket else _env_bucket()
    if s3_key is None:
        s3_key = make_key(local_path, s3_prefix=s3_prefix)
    size = os.path.getsize(local_path)
    hasher = None
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

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            hasher = hashlib.sha256()
            obs.put(store, s3_key, _chunks())
            break
        except Exception:
            if attempt == _MAX_RETRIES:
                raise
            delay = _RETRY_BASE_DELAY * 2 ** (attempt - 1)
            logger.warning(
                "S3 upload attempt %d/%d for %s failed, retrying in %.1fs …",
                attempt, _MAX_RETRIES, s3_key, delay,
            )
            time.sleep(delay)
    checksum = hasher.hexdigest()
    logger.debug("Upload of %s successful", local_path)
    return bucket, s3_key, size, checksum


def download_file(
    s3_key: str,
    *,
    target_path: str | None = None,
    bucket: str,
    expected_checksum: str | None = None,
    progress_callback: Callable[[int], None] | None = None,
) -> str:
    """Download an object from S3 to a temporary file.

    Streams directly to disk so arbitrarily large files never need to
    fit in memory.  Returns the path to the temp file.  The caller is
    responsible for moving or deleting it.

    Parameters
    ----------
    s3_key
        Object key in the configured bucket.
    target_path
        Required.  The temp file is created next to this path with a
        ``.<hex>.tmp`` suffix, ensuring the rename to ``target_path``
        is always same-filesystem.  Must include a directory component;
        ``ValueError`` is raised for ``None`` or a bare filename rather
        than silently falling back to the system temp directory or cwd.
    bucket
        Bucket name (plain name or ``s3://bucket`` URI).
    expected_checksum
        Expected SHA-256 hex digest. ``ValueError`` is raised on mismatch.
    progress_callback
        Optional callable invoked with the byte count of each streamed chunk.
    """
    store = get_store(bucket=bucket)
    bucket = _normalise_bucket(bucket)
    logger.debug("S3 download: s3://%s/%s -> %s", bucket, s3_key, target_path)

    last_exc = None
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            result = obs.get(store, s3_key)
            if target_path is None:
                raise ValueError(
                    "download_file requires an explicit target_path; refusing "
                    "to fall back to a temporary directory."
                )
            target_dir = os.path.dirname(target_path)
            if not target_dir:
                raise ValueError(
                    f"target_path {target_path!r} has no directory component; "
                    "a dir-qualified (ideally absolute) path is required so the "
                    "temp file lands next to its destination."
                )
            os.makedirs(target_dir, exist_ok=True)
            # "<name>.<8 hex>.tmp" matches check_files' leftover pattern
            # (same naming as ssh.write_file), so debris from a crashed
            # download is deletable by --delete-extra; mkstemp's random
            # suffix is not hex and would be kept forever.
            tmp_path = os.path.join(
                target_dir,
                f"{os.path.basename(target_path)}.{uuid.uuid4().hex[:8]}.tmp",
            )
            fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, _FILE_MODE)
            os.fchmod(fd, _FILE_MODE)
            try:
                hasher = hashlib.sha256()
                with os.fdopen(fd, "wb") as fh:
                    for chunk in result.stream():
                        b = bytes(chunk)
                        hasher.update(b)
                        fh.write(b)
                        if progress_callback is not None:
                            progress_callback(len(b))
            except BaseException:
                os.unlink(tmp_path)
                raise

            actual = hasher.hexdigest()
            if expected_checksum is not None and actual != expected_checksum:
                os.unlink(tmp_path)
                raise ValueError(
                    f"S3 download checksum mismatch: expected "
                    f"{expected_checksum}, got {actual}"
                )
            logger.debug("Download of s3://%s/%s successful", bucket, s3_key)
            return tmp_path
        except ValueError:
            # Checksum mismatches are not transient — do not retry.
            raise
        except Exception as exc:
            last_exc = exc
            if attempt == _MAX_RETRIES:
                break
            delay = _RETRY_BASE_DELAY * 2 ** (attempt - 1)
            logger.warning(
                "S3 download attempt %d/%d for %s failed (%s), "
                "retrying in %.1fs …",
                attempt, _MAX_RETRIES, s3_key, exc, delay,
            )
            time.sleep(delay)
    raise last_exc  # type: ignore[misc]


def download_bytes(
    s3_key: str,
    *,
    target_path: str | None = None,
    bucket: str,
    expected_checksum: str | None = None,
    progress_callback: Callable[[int], None] | None = None,
) -> bytes:
    """Download an object from S3 and return its content as bytes.

    Thin wrapper around ``download_file`` for callers that need the data
    in memory (e.g. tests).  For production transfers prefer
    ``download_file`` which streams to disk.
    """
    tmp_path = download_file(
        s3_key, target_path=target_path,
        expected_checksum=expected_checksum,
        progress_callback=progress_callback, bucket=bucket,
    )
    try:
        with open(tmp_path, "rb") as fh:
            return fh.read()
    finally:
        os.unlink(tmp_path)


def delete(s3_key: str, *, bucket: str | None = None) -> None:
    """Delete an object from the configured S3 bucket.

    Parameters
    ----------
    s3_key
        Object key to delete.
    bucket
        Bucket name override; None uses ``NEXUS_TRANSFER_S3_BUCKET``.
    """
    store = get_store(bucket=bucket)
    try:
        obs.delete(store, s3_key)
        logger.debug("Deleted s3://%s", s3_key)
    except Exception as exc:
        logger.warning("Failed to delete s3://%s: %s", s3_key, exc)
