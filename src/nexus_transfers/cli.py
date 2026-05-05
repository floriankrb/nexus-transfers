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
    }

    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print("usage: nexus-transfers <command> [args...]\n")
        print("Commands:")
        print("  broker     Run the WebSocket relay broker (runs forever)")
        print("  monitor    Run a monitoring client (runs forever)")
        print("  server     Run a peer awaiting messages (runs forever)")
        print("  copy       Copy a remote directory to local via relay")
        print("  copy-ssh   Copy a local directory to a remote SSH target")
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
