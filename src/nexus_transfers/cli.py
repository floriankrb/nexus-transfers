"""Unified CLI entry point: ``nexus-transfers <command> [args...]``."""

import sys


def main():
    """Dispatch to the appropriate subcommand."""
    commands = {
        "broker": "nexus_transfers.broker:main",
        "monitor": "nexus_transfers.monitor:main",
        "server": "nexus_transfers.client:main",
        "copy": "nexus_transfers.copy:main",
        "copy-ssh": "nexus_transfers.copy_ssh:main",
        "copy-to-s3": "nexus_transfers.copy_s3:main_to",
        "copy-from-s3": "nexus_transfers.copy_s3:main_from",
        "check": "nexus_transfers.check:main",
        "check-files": "nexus_transfers.check_files:main",
        "check-files-ssh": "nexus_transfers.check_files_ssh:main",
        "check-files-s3": "nexus_transfers.check_files_s3:main",
        "kill": "nexus_transfers.kill:main",
    }

    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print("usage: nexus-transfers <command> [args...]\n")
        print("Commands:")
        print("  broker     Run the WebSocket relay broker (runs forever)")
        print("  monitor    Run a monitoring client (runs forever)")
        print("  server     Run a peer awaiting messages (runs forever)")
        print("  copy       Copy a remote directory to local via relay")
        print("  copy-ssh   Copy a local directory to a remote SSH target")
        print("  copy-to-s3    Copy a local file or directory to an S3 bucket")
        print("  copy-from-s3  Copy an S3 object or prefix to the local disk")
        print("  check      Verify S3 credentials (put/get/delete a test object)")
        print("  check-files      Verify a local copy against a remote nexus reference")
        print("  check-files-ssh  Verify a remote SSH copy against the local reference")
        print("  check-files-s3   Verify an S3 copy against the local reference")
        print("  kill       Kill connected clients by name/wildcard, or --all")
        sys.exit(0)

    command = sys.argv[1]
    if command not in commands:
        print(f"nexus-transfers: unknown command '{command}'", file=sys.stderr)
        print(f"Available commands: {', '.join(commands)}", file=sys.stderr)
        sys.exit(1)

    # Remove the subcommand from argv so argparse in each module sees only its own args
    sys.argv = [f"nexus-transfers {command}"] + sys.argv[2:]

    module_path, func_name = commands[command].rsplit(":", 1)
    import importlib
    module = importlib.import_module(module_path)
    getattr(module, func_name)()


if __name__ == "__main__":
    main()
