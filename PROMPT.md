i want a server to relay messages between two clients, everything in python
use websockets.
the server need to be multithreaded, and both client connections will have access to the same memory

always work only in  ~/work/transfers. 
use the venv in ~/work/transfers/.venv/bin/python3 , this is a uv venv.

Create a skill to describe this activity.

let's go futher now.

Each client should have a id given in the command line --name <id>
when sending a message, another client need to specify the target id.
each message must have a reply, that is printed by the original client.
communication between clients and server uses json.
for now let's have a simple example of a client add 1 to a parameter.


use another port for testing

we want only one client.py code and we want to every client to support many remote procedure
start with a dispatch table to have
- adder
- echo
the other client will call prefixing the function with target name, eg /send a.adder 42
every routine, always return a value to the caller. The value may be a exception if it happens in the function, the target client should not terminate, and the source client should get the exception message and callstack


make a python package called "transfer" with this code (with "src' folder and pyproject.toml). move thing around, server and client should be importable from another python program and also have the two cli (transfer-server and transfer-client)
I want an example.py that will use the functions from the new "transfer" lib calling send("a.adder", 42) and printing the result