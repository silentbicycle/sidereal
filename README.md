A [Redis][] library for Lua, with:

 * an optional non-blocking mode
 * pipelining
 * automatic reconnections (*disabled when pipelining!*)
 * Lua-style lists and sets
 * a proxy table interface to the database

As of Redis 2.0.2, all new commands are supported (except HMGET and HMSET,
which are temporarily broken due to a protocol change).

*Note:* Newer redis releases are not yet fully supported. There has been
a protocol change and several new commands added. I have not had time to
rework the library lately, as it works for my purposes. (I will get to
it, eventually.) If you need it to be fixed in the near future, patches
are welcome, or else contact me and we can make some sort of arrangement.

To connect to a Redis server, use:
    c = sidereal.connect(host, port [, pass_hook])
    c:set("x", 12345)
    print(c:get("x"))         --> "12345"

If a pass hook function is provided, sidereal will call it to defer
control whenever reading/writing on a socket would block. (Typically,
this would yield to a coroutine scheduler.)

The commands available are closely based on the official [command reference][].

Normal Redis commands return (nil, error) on error. If the connection is
closed, Sidereal will make one attempt to reconnect (unless pipelining).
If that fails, it will return (nil, "closed").

When pipelining, be sure to check the result of R:send_pipeline(). Rather
than trying to automagically reconnect and replay a bunch of arbitrary
commands (some of which may have already run), Sidereal just reports the
disconnection and leaves the commands queued. (They can be cleared with
R:clear_pipeline().) Also, pipelines should generally be contained
in a MULTI/EXEC transaction, so that the whole group is atomic.

The proxy interface provides syntactic sugar for basic usage:

    c = sidereal.connect()
    local pr = c:proxy()
    pr.key = "value"
    local value = pr.key
    pr.key = nil               -- del key
    pr.my_list = { "a", "b", "c", "d", "e" }
    pr.my_set = { a=true, b=true, c=true, d=true, e=true }
    pr.my_zset = { a=1, b=2, c=3, d=4, e=5 }

Redis commands run via a proxy() table use Lua's error() call, since it
isn't possible to do normal error checking on e.g. "var = proxy.key".

For further usage examples, see the API documentation and test suite
(which includes a translation of most of the official TCL test suite).

[redis]: http://code.google.com/p/redis/
[command reference]: http://code.google.com/p/redis/wiki/CommandReference
