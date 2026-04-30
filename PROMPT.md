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

i want now to update the protocol to transfer binary (example file in a/data.bin), avoid any encoding (no base64, etc)(i want to transfer the raw bytes)
the source client will call a.get_file(<path>) , the transfer will be chunked, and the source client will create the target file, update the example to ask for data.bin (to a) and write it (it should write it in ~/work/transfers/example/.)

implement also a a.list_dir rpc as well.
client will be configured with a list of allowed path for list_dir and get_file, ensure that the paths do not contain .. or have any such security issue (e.ge realpath is out in the allowed list).

add tqdm to the transfer

now, create a get_directory, that will copy recursively a full directory (it will use list_dir and get_file), it should resume interrupted transfers.

add an checksum to the protocol : compute the checksum as you transfer and check that both checksum match at the end of the transfer

implement parallel transfer for the get_directory

add a list_clients (answered by the server)

The server must be configurable in the client cli, with --server-url. I will use WebSocket urls. 

Please add the support for Basic Auth in the client, if $NEXUS_TRANSFERS_USER and $NEXUS_TRANSFERS_PASSWORD are set
use the dotenv package to get them. 


default should be non interactive. have the interactive prompt in the client only with --interactive. 

implement a copy from a client command cli (recursive): nexus-copy <remote-client>:<source-dir> <TARGET-DIR>


use rich for output (colors etc) and Cmd to manage commands in the promp in interactive mode.

implement list_dir with pagination. (1000 files)t 