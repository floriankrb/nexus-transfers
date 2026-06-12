"""CLI tool: verify a local copy against a remote nexus reference.

The remote nexus client is the reference. The tree is walked exactly like
``nexus-copy`` (paged ``list_dir``), but instead of downloading file content
the remote computes a hash (``hash_file``) which is compared against a
locally computed one.

Usage::

    nexus-transfers check-files --from a /remote/dir /local/dir
    nexus-transfers check-files --from a /remote/dir /local/dir --fix --delete-extra
"""

import argparse
import asyncio
import datetime
import logging
import os
import stat as _stat
import sys
import uuid

from rich.console import Console

from nexus_transfers.client import (
    _DEFAULT_URL,
    Client,
    PeerNotFoundError,
    RemoteError,
)
from nexus_transfers.client._io import _write_file
from nexus_transfers.config import cli_default
from nexus_transfers.dispatch import compute_file_hash

_logger = logging.getLogger(__name__)


class CheckFailedError(Exception):
    """Raised when the check cannot run (e.g. an incompatible reference)."""


def _parse_mode(value) -> int:
    """Parse an octal permission string such as ``600`` or ``0o755``."""
    try:
        return int(str(value), 8)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"invalid octal mode: {value!r}"
        ) from exc


class CheckReport:
    """Aggregates discrepancies and emits throttled monitor events.

    Parameters
    ----------
    emit :
        Async callable ``emit(message, status=..., **kwargs)`` used to send
        monitor events (may be a no-op).
    name : str
        Client name used as the message prefix.
    label : str
        Short label of the tree being checked.
    interval : float
        Minimum number of seconds between two monitor events.
    """

    def __init__(self, emit, name: str, label: str, interval: float = 30.0):
        self._emit = emit
        self._name = name
        self._label = label
        self._interval = interval
        self._loop = asyncio.get_running_loop()
        self._start = self._loop.time()
        self._last_report = self._start

        self.checked = 0
        self.total: int | None = None
        # kind -> list of (rel_path, detail, fix_applied_or_None)
        self.discrepancies: dict[str, list[tuple[str, str, str | None]]] = {
            "corrupt": [], "missing": [], "extra": [], "mode": [],
        }

    # -- recording ---------------------------------------------------------

    def add(self, kind: str, rel_path: str, detail: str = "",
            fix: str | None = None) -> None:
        """Record one discrepancy.

        Parameters
        ----------
        kind : str
            One of ``corrupt``, ``missing``, ``extra``, ``mode``.
        rel_path : str
            Path of the file relative to the checked root.
        detail : str
            Free-form description (e.g. the two hashes).
        fix : str or None
            Description of the fix applied, or None when not fixed.
        """
        self.discrepancies[kind].append((rel_path, detail, fix))
        if fix is None:
            _logger.warning("%s: %s %s", kind.upper(), rel_path, detail)
        else:
            _logger.warning("%s: %s %s — fixed (%s)",
                            kind.upper(), rel_path, detail, fix)

    @property
    def n_found(self) -> int:
        return sum(len(v) for v in self.discrepancies.values())

    @property
    def n_fixed(self) -> int:
        return sum(
            1 for v in self.discrepancies.values() for _, _, fix in v
            if fix is not None
        )

    @property
    def n_unfixed(self) -> int:
        return self.n_found - self.n_fixed

    @property
    def ok(self) -> bool:
        return self.n_unfixed == 0

    # -- reporting ---------------------------------------------------------

    def _counts(self) -> str:
        parts = [
            f"{len(v)} {kind}"
            for kind, v in self.discrepancies.items() if v
        ]
        if not parts:
            return "no discrepancy"
        joined = ", ".join(parts)
        if self.n_fixed:
            joined += f" ({self.n_fixed} fixed)"
        return joined

    async def maybe_report(self) -> None:
        """Emit a grouped progress event, at most once per interval."""
        now = self._loop.time()
        if now - self._last_report < self._interval:
            return
        self._last_report = now
        total = f"/{self.total}" if self.total is not None else ""
        await self._emit(
            f"{self._name}: checked {self.checked}{total} files in "
            f"{self._label}: {self._counts()}",
            status="warning" if self.n_unfixed else "progress",
            progress={
                "label": f"{self._name}: {self.checked} files checked",
                "value": self.checked,
                "maximum": self.total,
                "unit": "file",
                "discrepancies": self.n_found,
                "fixed": self.n_fixed,
            },
        )

    async def final_report(self) -> None:
        """Emit the end-of-check summary event."""
        elapsed = self._loop.time() - self._start
        summary = (
            f"{self._name}: check of {self._label} finished — "
            f"{self.checked} files in {elapsed:.1f}s, {self._counts()}"
        )
        await self._emit(
            summary,
            status="ok" if self.ok else "error",
            progress={
                "label": f"{self._name}: {self.checked} files checked",
                "value": self.checked,
                "maximum": self.total,
                "unit": "file",
                "discrepancies": self.n_found,
                "fixed": self.n_fixed,
            },
        )

    _MAX_LISTED = 50  # console lines; everything is still in the log

    def print_summary(self, console: Console) -> None:
        """Print a human-readable summary to *console*."""
        rows = [
            (kind, rel_path, detail, fix)
            for kind, entries in self.discrepancies.items()
            for rel_path, detail, fix in entries
        ]
        for kind, rel_path, detail, fix in rows[: self._MAX_LISTED]:
            state = f"[green]fixed ({fix})[/green]" if fix else "[red]NOT fixed[/red]"
            console.print(f"  [yellow]{kind}[/yellow] {rel_path} {detail} {state}")
        if len(rows) > self._MAX_LISTED:
            console.print(
                f"  [dim]… and {len(rows) - self._MAX_LISTED} more "
                f"(see the log for the full list)[/dim]"
            )
        if self.ok:
            console.print(
                f"[bold green]OK[/bold green] — {self.checked} file(s) checked, "
                f"{self._counts()}"
            )
        else:
            console.print(
                f"[bold red]FAILED[/bold red] — {self.checked} file(s) checked, "
                f"{self._counts()}, {self.n_unfixed} unfixed"
            )


def scan_local_files(root: str) -> dict[str, int]:
    """Walk *root* and return ``{relative_posix_path: size}`` for all files.

    Symlinks are not followed. Returns an empty dict when *root* does not
    exist.

    Parameters
    ----------
    root : str
        Local directory to walk.
    """
    files: dict[str, int] = {}

    def _scan(dirpath: str, prefix: str) -> None:
        try:
            entries = list(os.scandir(dirpath))
        except FileNotFoundError:
            return
        for entry in entries:
            rel = f"{prefix}/{entry.name}" if prefix else entry.name
            if entry.is_dir(follow_symlinks=False):
                _scan(entry.path, rel)
            elif entry.is_file(follow_symlinks=False):
                try:
                    files[rel] = entry.stat(follow_symlinks=False).st_size
                except OSError:
                    files[rel] = 0

    _scan(root, "")
    return files


class _DirectoryCheck:
    """Verify a local directory against a remote nexus reference.

    Parameters
    ----------
    client :
        Connected :class:`Client` used for RPC and monitor events.
    target : str
        Name of the remote nexus client (the reference).
    remote_path : str
        Path on the remote client to check against.
    local_path : str
        Local copy to verify.
    report : CheckReport
        Aggregator for discrepancies and monitor events.
    max_concurrent : int
        Maximum number of files checked in parallel.
    algo : str
        Hash algorithm name (``hashlib.new`` compatible).
    fix : bool
        Re-download corrupt or missing files instead of failing.
    delete_extra : bool
        Delete local files absent from the reference.
    fix_permissions : int or None
        Explicit permission bits (e.g. ``0o600``) to enforce on every local
        file. ``None`` (default) only reports drift against the reference.
    use_s3 : bool
        Stage fix downloads through S3 (mirrors ``nexus-copy``).
    s3_prefix : str or None
        S3 key prefix for fix downloads.
    chunk_size : int
        Binary chunk size for fix downloads when ``use_s3`` is False.
    """

    def __init__(self, client, target, remote_path, local_path, report, *,
                 max_concurrent=4, algo="md5", fix=False, delete_extra=False,
                 fix_permissions=None, use_s3=True, s3_prefix=None,
                 chunk_size=65536):
        self._client = client
        self._target = target
        self._remote_path = remote_path
        self._local_path = local_path
        self._report = report
        self._max_concurrent = max_concurrent
        self._algo = algo
        self._fix = fix
        self._delete_extra = delete_extra
        self._fix_permissions = fix_permissions
        self._use_s3 = use_s3
        self._s3_prefix = s3_prefix
        self._chunk_size = chunk_size

        self._label = os.path.basename(remote_path.rstrip("/")) or remote_path
        self._loop = asyncio.get_running_loop()
        self._remote_rel: set[str] = set()
        self._walked = 0

    # -- public entry point ------------------------------------------------

    async def run(self) -> None:
        """Walk the reference, compare every file, then scan for extras."""
        progress = self._client._progress
        walk_task = progress.add_task(
            f"[magenta]Listing {self._label}[/magenta]", total=None, unit="files",
        )
        check_task = progress.add_task(
            f"[cyan]Checking {self._label}[/cyan]", total=None, unit="files",
        )
        queue: asyncio.Queue = asyncio.Queue()

        async def _walk_and_enqueue():
            nonlocal walk_task
            await self._walk_remote(
                self._remote_path, self._local_path, "", queue, walk_task,
            )
            self._report.total = self._walked
            progress.update(check_task, total=self._walked)
            for _ in range(self._max_concurrent):
                await queue.put(None)
            progress.remove_task(walk_task)
            walk_task = None

        async def _worker():
            while True:
                item = await queue.get()
                if item is None:
                    return
                remote_file, local_file, rel = item
                await self._check_one(remote_file, local_file, rel)
                self._report.checked += 1
                progress.update(check_task, completed=self._report.checked)
                await self._report.maybe_report()

        try:
            await asyncio.gather(
                _walk_and_enqueue(),
                *[_worker() for _ in range(self._max_concurrent)],
            )
        finally:
            if walk_task is not None:
                progress.remove_task(walk_task)
            progress.remove_task(check_task)

        await self._scan_extras()
        await self._report.final_report()

    # -- remote walk ---------------------------------------------------------

    async def _walk_remote(self, remote_path, local_path, rel_prefix, queue,
                           walk_task):
        """Recursively walk the reference tree with paged ``list_dir`` calls."""
        offset = 0
        limit = 1000
        dirs = []
        while True:
            page = await self._list_dir_with_retry(remote_path, offset=offset,
                                                   limit=limit)
            for entry in page:
                name = entry["name"]
                remote_child = (
                    f"{remote_path}/{name}" if remote_path != "." else name
                )
                local_child = os.path.join(local_path, name)
                rel = f"{rel_prefix}/{name}" if rel_prefix else name
                if entry["type"] == "dir":
                    dirs.append((remote_child, local_child, rel))
                else:
                    self._walked += 1
                    self._remote_rel.add(rel)
                    self._client._progress.update(
                        walk_task, completed=self._walked,
                    )
                    await queue.put((remote_child, local_child, rel))
            if len(page) < limit:
                break
            offset += len(page)

        for remote_child, local_child, rel in dirs:
            await self._walk_remote(remote_child, local_child, rel, queue,
                                    walk_task)

    async def _list_dir_with_retry(self, remote_path, offset=0, limit=1000):
        """Fetch a page of directory entries, retrying on transient errors."""
        while True:
            try:
                return await self._client.send(
                    f"{self._target}.list_dir", remote_path,
                    offset=offset, limit=limit,
                )
            except (PeerNotFoundError, ConnectionError,
                    asyncio.TimeoutError) as exc:
                _logger.warning(
                    "Listing %s failed (%s), retrying in %.1fs …",
                    remote_path, exc, self._client.peer_delay,
                )
                await asyncio.sleep(self._client.peer_delay)

    # -- per-file check ------------------------------------------------------

    async def _check_one(self, remote_file, local_file, rel):
        """Compare one file (hash + permission bits) and apply fixes."""
        remote_info, local_hash = await asyncio.gather(
            self._remote_hash_with_retry(remote_file),
            self._local_hash(local_file),
        )

        if local_hash is None:
            fix = None
            if self._fix:
                await self._download(remote_file, local_file)
                # A repaired file must fully match the reference, mode
                # included (a fresh download gets umask-default bits).
                os.chmod(local_file, remote_info["mode"])
                fix = "downloaded"
            self._report.add("missing", rel, "not found locally", fix=fix)
        elif local_hash != remote_info["hash"]:
            fix = None
            if self._fix:
                await self._download(remote_file, local_file)
                os.chmod(local_file, remote_info["mode"])
                fix = "re-downloaded"
            self._report.add(
                "corrupt", rel,
                f"{self._algo} {local_hash} != {remote_info['hash']}",
                fix=fix,
            )

        if os.path.isfile(local_file):
            local_mode = _stat.S_IMODE(os.stat(local_file).st_mode)
            if self._fix_permissions is not None:
                # Explicit target mode: enforce it on the local copy.
                if local_mode != self._fix_permissions:
                    os.chmod(local_file, self._fix_permissions)
                    self._report.add(
                        "mode", rel,
                        f"local {local_mode:o} != required "
                        f"{self._fix_permissions:o}",
                        fix=f"chmod {self._fix_permissions:o}",
                    )
            else:
                # Detection only: report drift against the reference.
                remote_mode = remote_info.get("mode")
                if remote_mode is not None and local_mode != remote_mode:
                    self._report.add(
                        "mode", rel,
                        f"local {local_mode:o} != remote {remote_mode:o}",
                    )

    async def _remote_hash_with_retry(self, remote_file):
        """Ask the reference for the file hash, retrying on transient errors."""
        while True:
            try:
                return await self._client.send(
                    f"{self._target}.hash_file", remote_file, algo=self._algo,
                )
            except RemoteError as exc:
                if "unknown function" in str(exc):
                    raise CheckFailedError(
                        f"the reference client '{self._target}' does not "
                        "expose 'hash_file' — it runs an older "
                        "nexus-transfers. Upgrade nexus-transfers on the "
                        "reference side and restart its server process."
                    ) from exc
                raise
            except (PeerNotFoundError, ConnectionError,
                    asyncio.TimeoutError) as exc:
                _logger.warning(
                    "Hashing %s failed (%s), retrying in %.1fs …",
                    remote_file, exc, self._client.peer_delay,
                )
                await asyncio.sleep(self._client.peer_delay)

    async def _local_hash(self, local_file):
        """Hash *local_file* in a thread, or return None when missing."""
        if not os.path.isfile(local_file):
            return None
        return await self._loop.run_in_executor(
            None, compute_file_hash, local_file, self._algo,
        )

    # -- fixes ---------------------------------------------------------------

    async def _download(self, remote_file, local_file):
        """Re-download one file from the reference (same path as nexus-copy)."""
        os.makedirs(os.path.dirname(local_file) or ".", exist_ok=True)
        while True:
            try:
                if self._use_s3:
                    data = await self._client.send(
                        f"{self._target}.get_file", remote_file,
                        use_s3=True, s3_prefix=self._s3_prefix,
                        _local_target=local_file,
                    )
                else:
                    data = await self._client.send(
                        f"{self._target}.get_file", remote_file,
                        chunk_size=self._chunk_size, use_s3=False,
                    )
                break
            except (PeerNotFoundError, ConnectionError,
                    asyncio.TimeoutError) as exc:
                _logger.warning(
                    "Fix download of %s failed (%s), retrying in %.1fs …",
                    remote_file, exc, self._client.peer_delay,
                )
                await asyncio.sleep(self._client.peer_delay)
        if isinstance(data, str):
            await self._loop.run_in_executor(None, os.replace, data, local_file)
        else:
            await self._loop.run_in_executor(None, _write_file, local_file, data)

    # -- extra local files -----------------------------------------------------

    async def _scan_extras(self):
        """Find local files absent from the reference; delete them if asked."""
        local_files = await self._loop.run_in_executor(
            None, scan_local_files, self._local_path,
        )
        for rel in sorted(set(local_files) - self._remote_rel):
            fix = None
            if self._delete_extra:
                try:
                    os.remove(os.path.join(self._local_path, rel))
                    fix = "deleted"
                except OSError as exc:
                    _logger.warning("Could not delete extra file %s: %s",
                                    rel, exc)
            self._report.add("extra", rel, "not on the reference", fix=fix)
            await self._report.maybe_report()


async def check_files(name, broker_url, remote_client, source, target,
                      site=None, max_concurrent=4, algo="md5", fix=False,
                      delete_extra=False, fix_permissions=None, use_s3=True,
                      chunk_size=65536, quiet=False, on_monitor=None,
                      **client_kwargs) -> CheckReport:
    """Connect to the relay and verify a local copy against the reference.

    Parameters
    ----------
    name:
        Local nexus client name for this session.
    broker_url:
        WebSocket broker URL.
    remote_client:
        Name of the remote nexus client holding the reference data.
    source:
        Path on the remote client to check against.
    target:
        Local copy to verify.
    site:
        Optional site label used in console output and monitor messages.
    max_concurrent:
        Maximum number of files checked in parallel.
    algo:
        Hash algorithm (``hashlib.new`` name; default md5).
    fix:
        If True, re-download corrupt or missing files.
    delete_extra:
        If True, delete local files that are not on the reference.
    fix_permissions:
        Explicit permission bits (e.g. ``0o600``) to enforce on every local
        file; None (default) only reports drift against the reference.
    use_s3:
        If True (default), stage fix downloads through S3.
    chunk_size:
        Binary chunk size for fix downloads when ``use_s3`` is False.
    quiet:
        If True, suppress rich console output.
    on_monitor:
        Optional async callable invoked after each monitor event with the
        same ``(message, status=..., **kwargs)`` signature.
    **client_kwargs:
        Forwarded to :class:`~nexus_transfers.client.Client`.

    Returns
    -------
    CheckReport
        The filled-in report; ``report.ok`` is False when unfixed
        discrepancies remain.
    """
    target = os.path.expanduser(target)
    console = Console(quiet=quiet)
    async with Client(name, broker_url, **client_kwargs) as client:

        async def _emit(message, status=None, **kw):
            await client.monitor(message, status=status, **kw)
            if on_monitor is not None:
                await on_monitor(message, status=status, **kw)

        dest_label = f"{site}:{target}" if site else target
        if not quiet:
            console.print(
                f"Checking [yellow]{dest_label}[/yellow] against "
                f"[yellow]{remote_client}:{source}[/yellow]"
            )
        await _emit(
            f"{name}: starting check {dest_label} against "
            f"{remote_client}:{source}",
            status="progress",
        )

        s3_prefix = None
        if use_s3 and fix:
            ts = datetime.datetime.now(datetime.timezone.utc).strftime(
                "%Y-%m-%d-%H%M%S")
            s3_prefix = f"{ts}-{remote_client}-{name}-{uuid.uuid4()}"

        label = os.path.basename(source.rstrip("/")) or source
        report = CheckReport(_emit, name, label)
        check = _DirectoryCheck(
            client, remote_client, source, target, report,
            max_concurrent=max_concurrent, algo=algo, fix=fix,
            delete_extra=delete_extra, fix_permissions=fix_permissions,
            use_s3=use_s3, s3_prefix=s3_prefix, chunk_size=chunk_size,
        )
        await check.run()
        report.print_summary(console)
        return report


def main() -> None:
    """CLI entry point for ``nexus-transfers check-files``."""
    parser = argparse.ArgumentParser(
        description="Verify a local copy against a remote nexus reference "
                    "(hashes and permissions), optionally fixing it",
    )
    parser.add_argument(
        "--from", dest="remote_client", required=True,
        help="Name of the remote client holding the reference data",
    )
    parser.add_argument("source", help="Remote reference directory path")
    parser.add_argument("target", help="Local directory to verify")
    parser.add_argument(
        "--broker-url",
        default=cli_default("broker_url", "check_files", default=None),
        help=f"Broker WebSocket URL (default: {_DEFAULT_URL})",
    )
    parser.add_argument(
        "--name", default=cli_default("name", "check_files", default=None),
        help="Client name (default: auto-generated)",
    )
    parser.add_argument(
        "--site", default=cli_default("site", "check_files", default=None),
        help="Site label used in the auto-generated client name",
    )
    parser.add_argument(
        "--algo", default=cli_default("algo", "check_files", default="md5"),
        help="Hash algorithm (default: md5)",
    )
    parser.add_argument(
        "--fix", action="store_true",
        default=cli_default("fix", "check_files", default=False),
        help="Re-download corrupt or missing files instead of failing",
    )
    parser.add_argument(
        "--delete-extra", action="store_true",
        default=cli_default("delete_extra", "check_files", default=False),
        help="Delete local files that are not on the reference",
    )
    parser.add_argument(
        "--fix-permissions", metavar="MODE", type=_parse_mode,
        default=cli_default("fix_permissions", "check_files", default=None,
                            type_fn=_parse_mode),
        help="Octal permission bits to enforce on every local file "
             "(e.g. 600); without this option drift is only reported",
    )
    parser.add_argument(
        "--max-concurrent", type=int,
        default=cli_default("max_concurrent", "check_files", default=4,
                            type_fn=int),
        help="Maximum parallel file checks (default: 4)",
    )
    parser.add_argument(
        "--use-broker", action="store_true",
        default=cli_default("use_broker", "check_files", default=False),
        help="Fix downloads via the WebSocket relay instead of S3 staging",
    )
    parser.add_argument(
        "--chunk-size", type=int,
        default=cli_default("chunk_size", "check_files", default=65536,
                            type_fn=int),
        help="Binary chunk size for fix downloads via the relay (default: 65536)",
    )
    parser.add_argument(
        "--peer-retries", type=int,
        default=cli_default("peer_retries", "check_files", default=-1,
                            type_fn=int),
        help="Retries when target peer is not found (-1 = infinite, default: -1)",
    )
    parser.add_argument(
        "--peer-delay", type=float,
        default=cli_default("peer_delay", "check_files", default=2.0,
                            type_fn=float),
        help="Seconds between peer-not-found retries (default: 2.0)",
    )
    parser.add_argument(
        "--call-timeout", type=float,
        default=cli_default("call_timeout", "check_files", default=None,
                            type_fn=float),
        help="Timeout in seconds for RPC calls (default: no timeout)",
    )
    parser.add_argument(
        "--no-verify", action="store_true",
        default=cli_default("no_verify", "check_files", default=False),
        help="Skip TLS certificate verification for wss:// connections",
    )
    parser.add_argument(
        "--debug", action="store_true",
        default=cli_default("debug", "check_files", default=False),
        help="Enable debug logging",
    )
    args = parser.parse_args()

    from rich.logging import RichHandler
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        handlers=[RichHandler(rich_tracebacks=True)],
    )

    tag = args.site or "check"
    name = args.name or f"{tag}-{uuid.uuid4().hex[:8]}"

    try:
        report = asyncio.run(
            check_files(
                name=name,
                broker_url=args.broker_url,
                remote_client=args.remote_client,
                source=args.source,
                target=args.target,
                site=args.site,
                max_concurrent=args.max_concurrent,
                algo=args.algo,
                fix=args.fix,
                delete_extra=args.delete_extra,
                fix_permissions=args.fix_permissions,
                use_s3=not args.use_broker,
                chunk_size=args.chunk_size,
                reconnect_retries=-1,
                peer_retries=args.peer_retries,
                peer_delay=args.peer_delay,
                call_timeout=args.call_timeout,
                ssl_verify=not args.no_verify,
            )
        )
    except CheckFailedError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(2)
    if not report.ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
