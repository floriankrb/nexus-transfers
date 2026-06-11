#!/usr/bin/env python3
"""Example: use the transfer library to call remote functions and transfer files.

Prerequisites
-------------
1. Start the broker::

       nexus-transfers broker

2. Start a client named "a" in another terminal, exposing a directory::

       nexus-transfers server --name a --allow-path .

3. Run this script::

       python example.py

The directory copy at the end uses ``use_s3=False`` so no S3 bucket is
needed; drop that argument to stage the transfer through S3 (requires
``NEXUS_TRANSFER_S3_BUCKET`` on client "a").
"""

import asyncio
import os

from nexus_transfers import Client


async def main():
    """Connect as 'example', call RPC functions, and transfer a file."""
    async with Client("example") as client:
        # add list clients
        clients = await client.list_clients()
        print(f"Connected clients: {', '.join(clients)}")

        # RPC calls
        result = await client.send("a.adder", 42)
        print(f"a.adder(42) = {result}")

        result = await client.send("a.echo", "hello world")
        print(f"a.echo('hello world') = {result}")

        # List directory on client a
        entries = await client.send("a.list_dir", ".")
        print("a.list_dir('.') =")
        for e in entries:
            suffix = f"  ({e['size']} bytes)" if e["type"] == "file" else "/"
            print(f"  {e['name']}{suffix}")

        # Recursive directory copy (resumes interrupted transfers)
        out_dir = os.path.expanduser("~/work/transfers/example/mirror")
        await client.get_directory("a", "src", out_dir, use_s3=False)
        print(f"Copied remote 'src' -> {out_dir}")

if __name__ == "__main__":
    asyncio.run(main())
