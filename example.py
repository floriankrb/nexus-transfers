#!/usr/bin/env python3
"""Example: use the transfer library to call a remote function.

Prerequisites
-------------
1. Start the server::

       transfer-server

2. Start a client named "a" in another terminal::

       transfer-client --name a

3. Run this script::

       python example.py
"""

import asyncio

from transfer import Client


async def main():
    """Connect as 'example', call a.adder(42), print the result."""
    async with Client("example") as client:
        result = await client.send("a.adder", 42)
        print(f"a.adder(42) = {result}")

        result = await client.send("a.echo", "hello world")
        print(f"a.echo('hello world') = {result}")


if __name__ == "__main__":
    asyncio.run(main())
