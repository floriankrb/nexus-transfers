Read an optional ~/.nexus-transfer.toml

Look at the code, and for all environment variables start starts with NEXT_TRANSFER_  create an equivalent entry in the config (in lowercase,
without the prefix).

For each tool (nexus-client, nexus-copy, etc) read the default cli arguments from the corresponding block (i.e. [client], [copy], [server], ...) in the config.

The precedence is always: cli option > environment variable > config > default

Also, you have seen the code, where we have a server, a client, tools (which are clients). From a higher perspective, the "server" is more a "broker".
The server brokers requests between clients (aka peers), so depending on the command, one can see consider the other peer as a "server". We also use the name "source" and "target". What names do you think would be more approriate? Please write your answer in NAMING.md. Do NOT rename anything.
