"""CLI tool: verify an S3 copy against the local reference.

The local directory is the reference. By default sizes are compared using a
single bucket listing; with ``--hash`` every object is streamed back from S3
and hashed, which re-downloads every byte but catches silent corruption.

Usage::

    nexus-transfers check-files-s3 --source /data/dataset.zarr --target s3://bucket/datasets/dataset.zarr
    nexus-transfers check-files-s3 --source ... --target ... --hash md5 --fix --delete-extra
"""

import asyncio
import hashlib
import logging
import os
import uuid
from typing import Callable

import obstore as obs
from nexus_transfers import s3
from nexus_transfers._progress import setup_cli_logging
from nexus_transfers.check_files import (
    CheckReport,
    check_copy,
)
from nexus_transfers._cli import CommandParser, run_check
from nexus_transfers.dispatch import compute_file_hash

_logger = logging.getLogger(__name__)


def _hash_remote(bucket: str, key: str, algo: str) -> str | None:
    """Stream an object from S3 and return its hex digest (None if missing)."""
    store = s3.get_store(bucket=bucket)
    hasher = hashlib.new(algo)
    try:
        result = obs.get(store, key)
        for chunk in result.stream():
            hasher.update(bytes(chunk))
    except FileNotFoundError:
        return None
    return hasher.hexdigest()


class _S3Copy:
    """An S3 copy ``s3://bucket[/prefix]`` of the local reference *local_root*
    (a :class:`~nexus_transfers.check_files.CopyTarget`).

    Compares sizes, or *algo* hashes (re-downloading every object); with
    *fix*, missing or corrupt objects are uploaded again.
    """

    def __init__(self, target: str, local_root: str, *,
                 algo: str | None = None, fix: bool = False) -> None:
        self.bucket, self.prefix = s3.parse_s3_url(target)
        self.local_root = os.path.expanduser(local_root)
        self.algo = algo
        self.fix = fix
        self._sizes: dict[str, int] = {}

    async def __aenter__(self) -> "_S3Copy":
        return self

    async def __aexit__(self, *exc) -> None:
        pass

    def _key(self, rel: str) -> str:
        return f"{self.prefix}/{rel}" if self.prefix else rel

    async def _run(self, fn, *args):
        return await asyncio.get_running_loop().run_in_executor(None, fn, *args)

    async def list_files(self) -> set[str]:
        listing = await self._run(s3.list_objects, self.bucket, self.prefix)
        skip = len(self.prefix) + 1 if self.prefix else 0
        self._sizes = {key[skip:]: size for key, size in listing if key[skip:]}
        return set(self._sizes)

    async def _upload(self, rel: str) -> None:
        await self._run(lambda: s3.upload_file(
            os.path.join(self.local_root, rel), s3_key=self._key(rel),
            bucket=self.bucket,
        ))

    async def check_one(self, rel: str, local_size: int,
                        report: CheckReport) -> bool:
        if rel not in self._sizes:
            fix_label = None
            if self.fix:
                await self._upload(rel)
                fix_label = "uploaded"
            report.add("missing", rel, "not found on S3", fix=fix_label)
            return True

        if self.algo is not None:
            remote_digest, local_digest = await asyncio.gather(
                self._run(_hash_remote, self.bucket, self._key(rel), self.algo),
                self._run(compute_file_hash,
                          os.path.join(self.local_root, rel), self.algo),
            )
            if remote_digest == local_digest:
                return True
            detail = f"{self.algo} remote {remote_digest} != local {local_digest}"
        else:
            remote_size = self._sizes[rel]
            if remote_size == local_size:
                return True
            detail = f"size remote {remote_size} != local {local_size}"

        fix_label = None
        if self.fix:
            await self._upload(rel)
            fix_label = "re-uploaded"
        report.add("corrupt", rel, detail, fix=fix_label)
        return True

    async def delete(self, rel: str) -> None:
        await self._run(lambda: s3.delete(self._key(rel), bucket=self.bucket))


async def _check_s3(
    source: str,
    target: str,
    broker_url: str | None,
    name: str,
    site: str | None,
    *,
    algo: str | None = None,
    fix: bool = False,
    delete_extra: bool = False,
    max_concurrent: int = 8,
    ssl_verify: bool = True,
    on_monitor: Callable | None = None,
    quiet: bool = False,
) -> CheckReport:
    """Verify the S3 *target* against the local *source* reference.

    Parameters
    ----------
    source : str
        Local reference directory.
    target : str
        S3 copy to verify: ``s3://bucket[/prefix]``.
    broker_url : str or None
        Relay WebSocket URL for monitoring only; ``None`` disables monitoring.
    name : str
        Client name on the relay.
    site : str or None
        Site label for monitor messages.
    algo : str or None
        Hash algorithm (any :func:`hashlib.new` name). None (default)
        compares sizes only; hashing re-downloads every byte.
    fix : bool
        Re-upload corrupt or missing objects instead of failing.
    delete_extra : bool
        Delete whitelisted extra objects (failed-transfer leftovers and
        ``_build/*``, see :func:`is_deletable_extra`); other extras are
        only reported, never deleted.
    max_concurrent : int
        Maximum number of files checked in parallel.
    ssl_verify : bool
        Verify TLS certificate for the relay connection.
    on_monitor : callable, optional
        Async callback invoked with ``(message, status=..., **kwargs)`` for
        every monitor event.
    quiet : bool
        If True, suppress rich console output (monitor events still fire).

    Returns
    -------
    CheckReport
        The filled-in report; ``report.ok`` is False when unfixed
        discrepancies remain.
    """
    return await check_copy(
        source, _S3Copy(target, source, algo=algo, fix=fix),
        f"{site}:{target}" if site else target,
        broker_url=broker_url, name=name, delete_extra=delete_extra,
        max_concurrent=max_concurrent, ssl_verify=ssl_verify,
        on_monitor=on_monitor, quiet=quiet,
    )


def main() -> None:
    """CLI entry point for ``nexus-transfers check-files-s3``."""
    parser = CommandParser(
        "check_files_s3",
        description="Verify an S3 copy against the local reference "
                    "(sizes by default, hashes with --hash), optionally "
                    "fixing it",
    )
    parser.add_argument("--source", required=True,
                        help="Local reference directory")
    parser.add_argument(
        "--target", required=True,
        help="S3 copy to verify: s3://bucket[/prefix]",
    )
    parser.monitor_options()
    parser.option("--hash", dest="algo", metavar="ALGO", key="hash",
                  help="Hash algorithm (e.g. md5, sha256); streams every "
                       "object back from S3, so every byte is re-downloaded "
                       "(default: compare sizes only)")
    parser.option("--fix", action="store_true",
                  help="Re-upload corrupt or missing objects instead of failing")
    parser.option(
        "--delete-extra", action="store_true",
        help="Delete whitelisted extra objects: failed-transfer leftovers "
             "(<base>.<hex>.tmp with <base> in the local reference) and "
             "objects under _build/; other extras are only reported, "
             "never deleted",
    )
    parser.option("--max-concurrent", type=int, default=8,
                  help="Maximum parallel file checks (default: 8)")
    parser.debug_option()
    args = parser.parse_args()

    setup_cli_logging(debug=args.debug)

    # e.g. "lumi-00085e89-check-s3": same prefix as the copy client of
    # that site, with a suffix telling the monitor what this client does.
    prefix = f"{args.site}-" if args.site else ""
    name = args.name or f"{prefix}{uuid.uuid4().hex[:8]}-check-s3"

    run_check(
        _check_s3(
            source=args.source,
            target=args.target,
            broker_url=args.broker_url,
            name=name,
            site=args.site,
            algo=args.algo,
            fix=args.fix,
            delete_extra=args.delete_extra,
            max_concurrent=args.max_concurrent,
            ssl_verify=not args.no_verify,
        )
    )


if __name__ == "__main__":
    main()
