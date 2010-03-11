A [Redis][] library for Lua, with:

 * an optional non-blocking mode
 * pipelining
 * automatic reconnections (disabled when pipelining)
 * Lua-style lists and sets
 * a proxy table interface to the database

As of Redis 1.2.2, all commands are supported.

To connect to a Redis server, use:
    c = sidereal.connect(host, port [, pass_hook])
    c:set("x", 12345)
    print(c:get("x"))         --> "12345"

If a pass hook function is provided, sidereal will call it to defer
control whenever reading/writing on a socket would block. (Typically,
this would yield to a coroutine scheduler.)

The proxy table provides syntactic sugar for basic usage:

    c = sidereal.connect()
    local pr = c:proxy()
    pr.key = "value"
    local value = pr.key
    pr.key = nil               -- del key
    pr.my_list = { "a", "b", "c", "d", "e" }
    pr.my_set = { a=true, b=true, c=true, d=true, e=true }
    pr.my_zset = { a=1, b=2, c=3, d=4, e=5 }

Normal Redis commands return (false, error) on error. If the connection
is closed, Sidereal will make one attempt to reconnect. If that fails,
it will return (false, "closed"). Redis commands run via a proxy() table
use Lua's error() call, since it isn't possible to do normal error
checking on e.g. "var = proxy.key".

For further usage examples, see the API documentation and test suite
(which includes a complete translation of the official TCL test suite).

[redis]: http://code.google.com/p/redis/
