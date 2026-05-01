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
