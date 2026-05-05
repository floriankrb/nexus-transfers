

when you download a file to disk, make sure it is downloaded in a temp file, not directly in the final filename, then rename atomically to the final file. we don't want truncated files.

nexus-copy should resume and interrupted transfers.
when resuming a transfer, print (log.info) the number of skipped files, etc.
when resuming, consider that if the file is present with the correct name, this is the correct file.
if --size has been passed, you can check this size information in addition.


the clients should always reconnect to the server when disconnected, with one exception : if another client is registered with the same name. the number of retry will be controlled with the cli with -1 meaning infinity. sleep time between retries is also controlled by an option

the client should always wait and retry when calling a peer that is not yet registered. the number of retry will be controlled with the cli with -1 meaning infinity.  sleep time between retries is also controlled by an option 

there must be a timeout when calling for a peer and waiting for a reply.
