One or more client will be able to register as monitoring service.
Each monitor messages (called events) will be broadcast to all monitoring services.
No replies will be given to the monitoring messages.


the broker will issue a monitoring events when a new client connects and a client a client is disconnected

write nexus-monitor cli that will register as a monitoring and print the broadcasted messages in the terminal.

Events have the following format (all dates in ISO UTC):

```json
{
    "type": "...",
    "date": "...",
    "source: "... name of client ...",
    "task": {
        "name": "...",
        "uuid": uuid,
        ... more entries related to each kind of task ...
    },
    "message": "Some text",
    "progress": {
        "label": "...",
        "uuid": uuid,
        "start": "... start date ... ",
        "update": "... update date ... ",
        "minimum": 1,
        "maximum": 123,
        "value": 27,
        "unit": "byte"
        "rate": 2.4,
    }
}
```

progress can be missing for simple messages
All events with the same progress uuid refer to the same progress

We start with the following events
- Registered to broker
- End
- Messages to peers (e.g. list_dir, get_file, ...)
- Data transfer progress (only top level progress)
- Errors
- Warnings


## `on_monitor` callback

Both `copy` and `copy_ssh` accept an optional `on_monitor` async callback.
This allows callers to receive monitor events locally without subscribing
to the broker broadcast.

The callback signature is `async def on_monitor(message, status=None, **kwargs)`.

`status` is one of `"progress"`, `"ok"`, `"warning"`, or `None`.

When `status` is `"progress"`, `kwargs` contains a `progress` dict with
`total_transferred`, `files_done`, `files_skipped`, and `rate`.

Progress events are throttled (not emitted on every file).
