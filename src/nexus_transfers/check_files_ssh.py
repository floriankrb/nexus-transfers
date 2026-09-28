"""CLI tool: verify a remote SSH copy against the local reference.

The local directory is the reference. Local files are walked and hashed,
the remote hash is computed over SSH (``md5sum`` by default, same asyncssh
pool as ``nexus-transfers copy-ssh``), and the two are compared.

Usage::

    nexus-transfers check-files-ssh --source /data/dataset.zarr --target user@host:/remote/path
    nexus-transfers check-files-ssh --source ... --target ... --fix --delete-extra
"""

import asyncio
import logging
import os
import stat as _stat
import time
import uuid
from typing import Callable

from nexus_transfers._progress import setup_cli_logging
from nexus_transfers.check_files import (
    CheckReport,
    _parse_age,
    _parse_mode,
    check_copy,
    scan_local_files,
)
from nexus_transfers._cli import CommandParser, run_check
from nexus_transfers.dispatch import compute_file_hash
from nexus_transfers.ssh import (
    SSHConfig,
    SSHPool,
    parse_ssh_target,
    remote_hash,
    walk_remote,
    write_file,
)

_logger = logging.getLogger(__name__)


class _SSHCopy:
    """A remote copy ``[user@]host:/path`` of the local reference
    *local_root* (a :class:`~nexus_transfers.check_files.CopyTarget`).

    Compares *algo* hashes (the remote host needs ``<algo>sum``) and
    permission bits.  With *fix*, missing or corrupt files are uploaded
    again with the reference's mode; *fix_permissions* (e.g. ``0o600``)
    is enforced on every file, else mode drift is only reported.  With
    *max_age* (seconds), remote files modified longer ago are skipped.
    """

    def __init__(self, target: str, local_root: str, *, algo: str = "md5",
                 fix: bool = False, fix_permissions: int | None = None,
                 max_age: float | None = None, ssh_port: int = 22,
                 ssh_key: str | None = None, ssh_connections: int = 2,
                 encryption_algs: list[str] | None = None) -> None:
        user, self.host, remote_base = parse_ssh_target(target)
        self.remote_base = remote_base.rstrip("/")
        self.local_root = os.path.expanduser(local_root)
        self.algo = algo
        self.fix = fix
        self.fix_permissions = fix_permissions
        self.max_age = max_age
        self._pool = SSHConfig(self.host, user, ssh_port, ssh_key,
                               ssh_connections, encryption_algs).pool()

    async def __aenter__(self) -> "_SSHCopy":
        await self._pool.connect()
        return self

    async def __aexit__(self, *exc) -> None:
        await self._pool.close()

    async def list_files(self) -> set[str]:
        return {rel for rel, _ in
                await walk_remote(self._pool.get_sftp(), self.remote_base)}

    async def delete(self, rel: str) -> None:
        await self._pool.get_sftp().remove(f"{self.remote_base}/{rel}")

    async def check_one(self, rel: str, local_size: int,
                        report: CheckReport) -> bool:
        local_file = os.path.join(self.local_root, rel)
        remote_file = f"{self.remote_base}/{rel}"
        sftp = self._pool.get_sftp()
        local_mode = _stat.S_IMODE(os.stat(local_file).st_mode)

        if self.max_age is not None:
            try:
                attrs = await sftp.stat(remote_file)
            except Exception:
                attrs = None  # missing remote file: never skip, check it
            if (attrs is not None and attrs.mtime is not None
                    and time.time() - attrs.mtime > self.max_age):
                report.skipped += 1
                return False

        remote_digest, local_digest = await asyncio.gather(
            remote_hash(self._pool.get_conn(), remote_file, algo=self.algo),
            asyncio.get_running_loop().run_in_executor(
                None, compute_file_hash, local_file, self.algo),
        )

        if remote_digest is None:
            problem = ("missing", "not found on remote", "uploaded")
        elif remote_digest != local_digest:
            problem = ("corrupt",
                       f"{self.algo} remote {remote_digest} != local {local_digest}",
                       "re-uploaded")
        else:
            problem = None
        if problem is not None:
            kind, detail, fixed = problem
            fix_label = None
            if self.fix:
                await write_file(sftp, local_file, remote_file)
                # A repaired file must fully match the reference, mode
                # included (a fresh upload gets server-default bits).
                await sftp.chmod(remote_file, local_mode)
                fix_label = fixed
            report.add(kind, rel, detail, fix=fix_label)
            if remote_digest is None and not self.fix:
                return True  # nothing on the remote to compare modes against

        try:
            attrs = await sftp.stat(remote_file)
        except Exception:
            return True
        remote_mode = _stat.S_IMODE(attrs.permissions)
        if self.fix_permissions is not None:
            # Explicit target mode: enforce it on the remote copy.
            if remote_mode != self.fix_permissions:
                await sftp.chmod(remote_file, self.fix_permissions)
                report.add(
                    "mode", rel,
                    f"remote {remote_mode:o} != required {self.fix_permissions:o}",
                    fix=f"chmod {self.fix_permissions:o}",
                )
        elif remote_mode != local_mode:
            # Detection only: report drift against the local reference.
            report.add(
                "mode", rel,
                f"remote {remote_mode:o} != local {local_mode:o}",
            )
        return True


async def _check_ssh(
    source: str,
    target: str,
    broker_url: str | None,
    name: str,
    site: str | None,
    *,
    algo: str = "md5",
    fix: bool = False,
    delete_extra: bool = False,
    fix_permissions: int | None = None,
    max_concurrent: int = 4,
    ssh_port: int = 22,
    ssh_key: str | None = None,
    ssh_connections: int = 2,
    ssl_verify: bool = True,
    encryption_algs: list[str] | None = None,
    max_age: float | None = None,
    on_monitor: Callable | None = None,
    quiet: bool = False,
) -> CheckReport:
    """Verify the SSH *target* against the local *source* reference.

    Parameters
    ----------
    source : str
        Local reference directory.
    target : str
        Remote copy in the form ``[user@]host:/path``.
    broker_url : str or None
        Relay WebSocket URL for monitoring only; ``None`` disables monitoring.
    name : str
        Client name on the relay.
    site : str or None
        Site label for monitor messages.
    algo : str
        Hash algorithm; the remote host needs a ``<algo>sum`` binary.
    fix : bool
        Re-upload corrupt or missing remote files instead of failing.
    delete_extra : bool
        Delete whitelisted extra remote files (failed-transfer leftovers
        and ``_build/*``, see :func:`is_deletable_extra`); other extras
        are only reported, never deleted.
    fix_permissions : int or None
        Explicit permission bits (e.g. ``0o600``) to enforce on every remote
        file; None (default) only reports drift against the local reference.
    max_concurrent : int
        Maximum number of files checked in parallel.
    ssh_port : int
        SSH port on the target host.
    ssh_key : str or None
        Path to the SSH private key file.
    ssh_connections : int
        Number of SSH connections to open in the pool.
    ssl_verify : bool
        Verify TLS certificate for the relay connection.
    encryption_algs : list of str or None
        SSH cipher preference list.
    max_age : float or None
        Only check remote files modified within the last ``max_age``
        seconds; older files are skipped. None (default) checks everything.
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
    copy = _SSHCopy(
        target, source, algo=algo, fix=fix, fix_permissions=fix_permissions,
        max_age=max_age, ssh_port=ssh_port, ssh_key=ssh_key,
        ssh_connections=ssh_connections, encryption_algs=encryption_algs,
    )
    return await check_copy(
        source, copy,
        f"{site or copy.host}:{copy.remote_base}",
        broker_url=broker_url, name=name, delete_extra=delete_extra,
        max_concurrent=max_concurrent, ssl_verify=ssl_verify,
        on_monitor=on_monitor, quiet=quiet,
    )


async def _verify_ssh(
    target: str,
    expected,
    *,
    checksum: bool = False,
    algo: str = "md5",
    tally=None,
    ssh_port: int = 22,
    ssh_key: str | None = None,
    ssh_connections: int = 2,
    max_concurrent: int = 4,
    encryption_algs: list[str] | None = None,
) -> dict:
    """Compare the SSH *target* against what is *expected* — read-only.

    The comparison of ``check-files-ssh`` as a result instead of a report:
    nothing is fixed or deleted, and nothing at *target* is not an error
    (everything is then missing).

    Parameters
    ----------
    target : str
        ``[user@]host:/path`` — a directory or a single file.
    expected : str, os.PathLike or Mapping[str, int]
        A local reference (directory or single file), or a manifest
        ``{relative_posix_path: size}``.  A single-file reference is the
        one-entry manifest ``{basename: size}``; a single-file *target* is
        compared as the manifest's only file.
    checksum : bool
        Also compare ``algo`` hashes of same-size files (needs a local
        reference; ignored for a manifest).
    tally : nexus_transfers._progress.Tally, optional
        Advanced by the expected size of each file compared.

    Returns
    -------
    dict
        ``{"bytes", "files", "missing", "mismatched", "extra"}``: the totals
        of what is at *target*, and sorted lists of relative paths.
    """
    from collections.abc import Mapping

    user, host, remote_base = parse_ssh_target(target)
    remote_base = remote_base.rstrip("/") or "/"
    local_root: str | None = None
    local_file: str | None = None
    if isinstance(expected, Mapping):
        manifest = {str(k): int(v) for k, v in expected.items()}
    else:
        ref = os.path.expanduser(os.fspath(expected))
        if os.path.isdir(ref):
            local_root = ref
            manifest = await asyncio.get_running_loop().run_in_executor(
                None, scan_local_files, ref)
        elif os.path.isfile(ref):
            local_file = ref
            manifest = {os.path.basename(ref): os.path.getsize(ref)}
        else:
            raise FileNotFoundError(f"reference {ref} does not exist")
    if tally is not None:
        tally.bytes_total = sum(manifest.values())

    async with SSHPool(host, ssh_port, user, ssh_key, ssh_connections,
                       encryption_algs) as pool:
        sftp = pool.get_sftp()
        try:
            attrs = await sftp.stat(remote_base)
        except Exception:                 # asyncssh.SFTPError: nothing there
            attrs = None
        single = attrs is not None and not _stat.S_ISDIR(attrs.permissions or 0)
        if attrs is None:
            remote: dict[str, int] = {}
        elif single:
            key = (next(iter(manifest)) if len(manifest) == 1
                   else remote_base.rsplit("/", 1)[-1])
            remote = {key: int(attrs.size or 0)}
        else:
            remote = dict(await walk_remote(sftp, remote_base))

        missing = sorted(set(manifest) - set(remote))
        extra = sorted(set(remote) - set(manifest))
        mismatched: list[str] = []
        common = sorted(set(manifest) & set(remote))
        for rel in missing:
            if tally is not None:
                tally.add(manifest[rel])
        hash_it = checksum and (local_root is not None or local_file is not None)
        loop = asyncio.get_running_loop()
        sem = asyncio.Semaphore(max_concurrent)

        async def _compare(rel: str) -> None:
            if remote[rel] != manifest[rel]:
                mismatched.append(rel)
            elif hash_it:
                local = local_file or os.path.join(local_root, rel)
                remote_file = remote_base if single else f"{remote_base}/{rel}"
                async with sem:
                    remote_digest, local_digest = await asyncio.gather(
                        remote_hash(pool.get_conn(), remote_file, algo=algo),
                        loop.run_in_executor(None, compute_file_hash, local, algo),
                    )
                if remote_digest != local_digest:
                    mismatched.append(rel)
            if tally is not None:
                tally.add(manifest[rel])

        await asyncio.gather(*[_compare(rel) for rel in common])

    return {
        "bytes": sum(remote.values()),
        "files": len(remote),
        "missing": missing,
        "mismatched": sorted(mismatched),
        "extra": extra,
    }


def main() -> None:
    """CLI entry point for ``nexus-transfers check-files-ssh``."""
    parser = CommandParser(
        "check_files_ssh",
        description="Verify a remote SSH copy against the local reference "
                    "(hashes and permissions), optionally fixing it",
    )
    parser.add_argument("--source", required=True,
                        help="Local reference directory")
    parser.add_argument(
        "--target", required=True,
        help="Remote copy to verify: [user@]host:/remote/path",
    )
    parser.monitor_options()
    parser.option("--algo", default="md5",
                  help="Hash algorithm; the remote host needs <algo>sum "
                       "(default: md5)")
    parser.option("--fix", action="store_true",
                  help="Re-upload corrupt or missing remote files instead of "
                       "failing")
    parser.option(
        "--delete-extra", action="store_true",
        help="Delete whitelisted extra remote files: failed-transfer "
             "leftovers (<base>.<hex>.tmp with <base> in the local "
             "reference) and files under _build/; other extras are only "
             "reported, never deleted",
    )
    parser.option("--fix-permissions", metavar="MODE", type=_parse_mode,
                  help="Octal permission bits to enforce on every remote file "
                       "(e.g. 600); without this option drift is only reported")
    parser.option("--max-concurrent", type=int, default=4,
                  help="Maximum parallel file checks (default: 4)")
    parser.ssh_options()
    parser.option("--max-age", metavar="AGE", type=_parse_age,
                  help="Only check remote files modified within AGE — e.g. "
                       "30d, 1h, 45m, 4 (seconds); older files are skipped "
                       "(default: check all)")
    parser.debug_option()
    args = parser.parse_args()

    setup_cli_logging(debug=args.debug)

    # e.g. "lumi-00085e89-check-ssh": same prefix as the copy client of
    # that site, with a suffix telling the monitor what this client does.
    prefix = f"{args.site}-" if args.site else ""
    name = args.name or f"{prefix}{uuid.uuid4().hex[:8]}-check-ssh"

    run_check(
        _check_ssh(
            source=args.source,
            target=args.target,
            broker_url=args.broker_url,
            name=name,
            site=args.site,
            algo=args.algo,
            fix=args.fix,
            delete_extra=args.delete_extra,
            fix_permissions=args.fix_permissions,
            max_concurrent=args.max_concurrent,
            ssh_port=args.ssh_port,
            ssh_key=args.ssh_key,
            ssh_connections=args.ssh_connections,
            ssl_verify=not args.no_verify,
            encryption_algs=args.cipher,
            max_age=args.max_age,
        )
    )


if __name__ == "__main__":
    main()
