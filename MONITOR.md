

let's add a monitoring facility

there will be a fucntion called monitor, called by any peer, take a string and optional status. this will call the peer called monitor with the parameters, and if monitor does not run, the error is ignored (don't block), the imteout for the reply is very short and not an error.
write nexus-monitor cli that will register as 'monitor' and print the message string (and status)
let s add a call to monitor in nexus-copy that will send a message to report on the progress on the copy.


