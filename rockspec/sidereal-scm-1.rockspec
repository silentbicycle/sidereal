package = "sidereal"
version = "scm-1"

source = {
   url = "git://github.com/silentbicycle/sidereal.git"
}

description = {
   summary = "Redis library for Lua, with optional non-blocking mode and Lua-style lists & sets.",
   detailed = [[
      A Redis library for Lua, with:

      * an optional non-blocking mode
      * pipelining
      * automatic reconnections (disabled when pipelining)
      * Lua-style lists and sets
      * a proxy table interface to the database

      As of Redis 1.2.2, all commands are supported.
   ]],
   homepage = "http://github.com/silentbicycle/sidereal",
   license = "MIT/X11"
}

dependencies = {
   "lua >= 5.1",
   "luasocket"
}

build = {
   type = "none",
   install = {
      lua = {
         "sidereal.lua"
      }
   }
}
