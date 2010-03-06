A [Redis][] library for Lua, with optional *non-blocking* mode, Lua-style
lists & sets, and simple use via a proxy table. All commands as of Redis
1.2.2 are supported.

To connect to a Redis server, use:
    c = sidereal.connect(host, port[, pass_hook])

If a pass hook function is provided, sidereal will call it to defer
control whenever reading/writing on a socket would block. (Typically,
this would yield to a coroutine scheduler.)

Normal Redis commands return (false, error) on error. Mainly, watch for
(false, "closed") if the connection breaks. Redis commands run via a
proxy() table use Lua's error() call, since it isn't possible to do
normal error checking on e.g. "var = proxy.key".

For further usage examples, see the API documentation and test suite.

[redis]: http://code.google.com/p/redis/
