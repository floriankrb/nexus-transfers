I want the server to be less involved in the protocol, and only decode json wehen needed.
So, the messages on the web sockect will be as follows:

- 1 byte version, set to 1
- 1 byte length of source client name
- source client name

- 1 byte message name length
- message name

- 1 byte length of target client name, zero if message is for the server itself
- target client name (empty for the server)

- byte payload encoding J = JSON, R = raw
- 4 bytes payload size, network order

- payload

assert that client names are between 1 and 255


Please use the 'match' contruct, not if/elif/else block when appropritae


Add a progress bar in the dowload part of get_file. At the end of a get_directory, print the overall throughpout: elapsed, volume and rate. Always use binary sizes (TiB, not TB)

Remove the support for the "memory" message.

Make the chunking size of transfer configurable. The size is selected by the client that calls get_size, and passed to the remote client. Add a parameter to the nexus-copy cli
