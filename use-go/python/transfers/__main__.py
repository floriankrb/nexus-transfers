"""CLI entry point.  Run with:  python -m transfers  or the `transfers` script."""

from __future__ import annotations

import argparse
import logging
import sys

from transfers import copy, copy_many, serve, serve_loop
from transfers._progress import Progress

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="transfers",
        description="Relay-based file transfer client (files, directories, resume).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
spec format:
  machineA:/data/dir/        remote peer — file or directory
  ./local/dir/               local path
  me:/local/path             local path (explicit self-reference)

examples:
  # Machine A — serve 4 parallel slots:
  python -m transfers --relay relay.example.com --name machineA serve -n 4

  # Machine B — pull an entire directory:
  python -m transfers --relay relay.example.com --name machineB \\
      copy machineA:/data/ ./data/

  # Machine B — push 4 files in parallel:
  python -m transfers --relay relay.example.com --name machineB copy-many \\
      ./file0.tar machineA:/dest/file0.tar \\
      ./file1.tar machineA:/dest/file1.tar \\
      ./file2.tar machineA:/dest/file2.tar \\
      ./file3.tar machineA:/dest/file3.tar
""",
    )
    p.add_argument("--relay", required=True, metavar="HOST",
                   help="relay hostname, e.g. relay.example.com")
    p.add_argument("--name", required=True, metavar="NAME",
                   help="well-known name for this machine, e.g. machineA")
    p.add_argument("--port", type=int, default=443, metavar="PORT",
                   help="relay port (default 443)")
    p.add_argument("--insecure", action="store_true",
                   help="skip TLS certificate verification (dev/internal only)")
    p.add_argument("--no-progress", action="store_true",
                   help="disable the terminal progress bar")

    sub = p.add_subparsers(dest="cmd", required=True)

    # serve
    sp = sub.add_parser("serve",
                        help="register and handle incoming transfer(s)")
    sp.add_argument("-n", type=int, default=1, metavar="N",
                    help="number of parallel slots (default 1)")

    # copy
    cp = sub.add_parser("copy",
                        help="copy a file or directory tree")
    cp.add_argument("src", help="source spec, e.g. machineA:/data/ or ./data/")
    cp.add_argument("dst", help="destination spec")

    # copy-many
    cm = sub.add_parser("copy-many",
                        help="copy multiple items in parallel (interleaved src dst pairs)")
    cm.add_argument("--workers", type=int, default=None, metavar="N",
                    help="max parallel threads (default: one per pair)")
    cm.add_argument("pairs", nargs="+", metavar="ARG",
                    help="interleaved pairs: src0 dst0 [src1 dst1 …]")

    return p


def main(argv: list[str] | None = None) -> None:
    p = _build_parser()
    args = p.parse_args(argv)

    prog: Progress | None = None
    if not args.no_progress and sys.stderr.isatty():
        prog = Progress(sys.stderr)

    kw = dict(port=args.port, insecure=args.insecure, progress=prog)

    try:
        if args.cmd == "serve":
            log.info("serving as %r  (slots=%d)…", args.name, args.n)
            serve_loop(args.relay, args.name, args.n, **kw)
            log.info("all slots complete")

        elif args.cmd == "copy":
            copy(args.relay, args.name, args.src, args.dst, **kw)
            log.info("done")

        elif args.cmd == "copy-many":
            raw = args.pairs
            if len(raw) % 2 != 0:
                p.error("copy-many requires an even number of arguments (src dst pairs)")
            pairs = [(raw[i], raw[i + 1]) for i in range(0, len(raw), 2)]
            log.info("copying %d item(s) in parallel as %r…", len(pairs), args.name)
            copy_many(args.relay, args.name, pairs, workers=args.workers, **kw)
            log.info("done")
    finally:
        if prog is not None:
            prog.stop()


if __name__ == "__main__":
    main()
