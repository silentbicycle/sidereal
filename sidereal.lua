#!/usr/bin/env lua

------------------------------------------------------------------------
-- Copyright (c) 2009 Scott Vokes <scott@silentbicycle.com>
--
-- Permission to use, copy, modify, and/or distribute this software for any
-- purpose with or without fee is hereby granted, provided that the above
-- copyright notice and this permission notice appear in all copies.
--
-- THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
-- WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
-- MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
-- ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
-- WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
-- ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
-- OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
------------------------------------------------------------------------
--
-- To connect to a redis server, use:
--     c = redis.connect(host, port)  (defaults to "localhost", 6379)
--
-- To get a list of supported commands, use:
--     c:help()
--
------------------------------------------------------------------------


-------------------------
-- Module requirements --
-------------------------

local socket = require "socket"
local error, fmt, len = error, string.format, string.len
local sub, upper = string.sub, string.upper
local gmatch, find = string.gmatch, string.find
local concat, assert, type = table.concat, assert, type
local setmetatable, tonumber = setmetatable, tonumber
local ipairs, pairs, print, sort = ipairs, pairs, print, table.sort

module(...)

-- global null sentinel, to distinguish from Lua's nil
NULL = setmetatable( {}, {__tostring = function() return "NULL" end} )


-- Documentation table, filled in by cmd below.
local doctable = {}


----------------
-- Connection --
----------------

local Connection = {}           -- prototype
local ConnMT = { __index = Connection }

-- Print known functions, their arguments, and docstrings.
function Connection:help()
   local output = {}
   local keys = {}
   for key in pairs(doctable) do keys[#keys+1] = key end
   sort(keys)
   for _,key in ipairs(keys) do
      -- Pretty simple formatting...
      local docpair = doctable[key]
      output[#output+1] = fmt("%s (%s):\n  * %s\n", key, docpair[1], docpair[2])
   end
   print(concat(output, "\n"))
end


-- Create and return a connection object.
function connect(host, port)
   host = host or "localhost"
   port = port or 6379
   assert(type(host) == "string", "host must be string")
   assert(type(port) == "number" and port > 0 and port < 65536)

   local sock = socket.connect(host, port)
   sock:settimeout(0.1)         --FIXME
   if not sock then 
      error(fmt("Could not connect to %s port %d.", host, port)) 
   end

   -- Also putting help() in connection namespace, so it's extra-visible.
   local conn = { __socket = sock, 
                  host = host,
                  port = port,
                  help = Connection.help 
               }
   setmetatable(conn, ConnMT )
   return conn
end


-- Read a bulk value from a socket.
local function bulk(s)
   local val                             -- the value read
   local len = tonumber(s:receive"*l")   -- length response to read
   if len == -1 then return NULL else val = s:receive(len) end

   local nl = s:receive(2) == "\r\n"
   return val
end


-- Return an iterator for a sequence of bulk values read from a socket.
local function bulk_multi(s)
   local ct = tonumber(s:receive"*l")
   local function iter()
      while ct >= 1 do
         s:receive(1)
         ct = ct - 1
         return bulk(s)
      end
   end
   return iter, ct
end


-- Actions
local action = 
   { ["-"]=function(s) error('Redis: "' .. s:receive"*l" .. '"') end,
     ["+"]=function(s) return s:receive"*l" end,           -- one line
     ["$"]=bulk,
     ["*"]=bulk_multi,
     [":"]=function(s) return tonumber(s:receive"*l") end  -- integer
  }


-- Read and parse a response.
function Connection:receive()
   local sock = self.__socket

   local t, res = sock:receive(1)

   if t == nil then error(res) end

   return action[t](sock)
end


-- Send a command, don't wait for response.
function Connection:send(cmd)
   self.__socket:send(cmd .. "\r\n")
end


-- Send a command and return the response.
function Connection:sendrecv(cmd, handler)
   -- self:send(cmd)
   self.__socket:send(cmd .. "\r\n")
   local res = self:receive()
   if handler then
      return handler(res)
   else
      return res
   end 
end


------------------------
-- Command generation --
------------------------

-- Argument formatters, for sending.
local formatter = {}

function formatter.simple(val) return val end

function formatter.bulk(val)
   return fmt("%s\r\n%s\r\n", len(val), val)
end

function formatter.bulklist(list)
   local t = {}
   local bulk = formatter.bulk
   t[1] = "*" .. #list .. "\r\n"

   for i,v in ipairs(list) do
      t[i+1] = bulk(v)
   end

   return concat(t, "$")
end


-- Type tests
local tester = {}

function tester.todo() return true end


-- type key -> ( description, formatter function, test function )
local types = { k={ "key", formatter.simple, tester.todo },
                v={ "value", formatter.bulk, tester.todo },
                K={ "key list", formatter.bulklist, tester.todo }, --uppercase->list
                i={ "integer", formatter.simple, tester.todo },
                p={ "pattern", formatter.simple, tester.todo },
                n={ "name", formatter.simple, tester.todo },
                t={ "time", formatter.simple, tester.todo },
                s={ "start index", formatter.simple, tester.todo }, -- int
                e={ "end index", formatter.simple, tester.todo },   -- int
                m={ "member", formatter.bulk, tester.todo },      -- ??? key?
             }


-- Generate type and docstring pair for a command.
local function gen_doc(name, type_str, doc)
   local type_sig = {}

   for t in gmatch(type_str, ".") do
      local tpair = types[t] -- assert(types[t], "type not found: " .. t)
      local tk = tpair[1]  --type key
      if tk then type_sig[#type_sig+1] = tk end
   end

   return { concat(type_sig, ", "), doc }
end


-- Register a command.
local function cmd(fname, arg_types, name, doc, opts) 
   opts = opts or {}
   arg_types = arg_types or ""
   local typecheck_fun = typechecker(arg_types)
   local result_handler = opts.result_handler    -- optional

   Connection[name] = 
      function(self, ...) 
         local arglist = typecheck_fun(...)
         return self:sendrecv(fname .. " " .. arglist, result_handler) 
      end

   local docstring = gen_doc(name, arg_types, doc)
   -- print(docstring)
   doctable[name] = docstring
end


-- Generate a function to check and process arguments.
function typechecker(ats)
   local tt = {}   --type's (name, formatter, checker) tuple
   local types = types

   local adjs = {}
   for i=1,len(ats) do
      local tt = types[sub(ats, i, i)]
      if tt[2] ~= formatter.simple then
         adjs[#adjs+1] = { i, tt[2] }
      end
   end

   local adjust
   if #adjs > 0 then
      adjust = function(args)
                  for _,pair in ipairs(adjs) do
                     local idx, form = pair[1], pair[2]
                     args[idx] = form(args[idx])
                  end
                  return args
               end
   end

   return 
   function(...)
      arglist = { ... } or {}   --unprocessed args
      if adjust then arglist = adjust(arglist) end
      return concat(arglist, " ")
   end
end


---------------------
-- Result handlers --
---------------------

-- Convert an int reply of 0 or 1 to a boolean.
local function tobool(res)
   if res == 0 then return false
   elseif res == 1 then return true
   else 
      local err = fmt("Expected 0 or 1, got %s.", res)
      error(err)
   end
end


-- local function totable
-- local function tolist -> { val, val2, etc. }
-- local function toset -> { key=true, key2=true, etc. }


-- Make a table out of the output from c:info().
local function info_table(s)
   local t = {}

   for k,v in gmatch(s, "([^:]*):([^\r\n]*)\r\n") do   --split key:val
      if k and v then t[k] = v end
   end

   return t
end 


-- Split keys (which is a string, not a bulk_multi), return an iterator.
local function keys_iter(s)
   return gmatch(s, "([^ ]+) -")
end


-----------------------------
-- Table-style interface ? --
-----------------------------

-- conn[key]  -> val (or { arr val} or { set val }, as the case may be)
-- conn[key] = blah   -> SET or MSET etc., or DEL if nil
--
-- better interface for expire? the time val.
-- bulk SET via table and pairs?


------------------------
-- Generated commands --
------------------------

-- Connection handling
cmd("QUIT", "", "quit", "Close the connection")
cmd("AUTH", "k", "auth", "Simple password authentication if enabled")


-- Commands operating on string values
cmd("SET", "kv", "set", "Set a key to a string value")
cmd("GET", "k", "get", "Return the string value of the key")
cmd("GETSET", "kv", "getset", "Set a key to a string returning the old value of the key")
cmd("MGET", "K", "mget", "Multi-get, return the strings values of the keys")
cmd("SETNX", "kv", "setnx",
    "Set a key to a string value if the key does not exist", 
    { result_handler=tobool } )
cmd("INCR", "k", "incr", "Increment the integer value of key")
cmd("INCRBY", "ki", "incrby", "Increment the integer value of key by integer")
cmd("DECR", "k", "decr", "Decrement the integer value of key")
cmd("DECRBY", "ki", "decrby", "Decrement the integer value of key by integer")
cmd("EXISTS", "k", "exists", "Test if a key exists", 
    { result_handler=tobool } )
cmd("DEL", "k", "del", "Delete a key")
cmd("TYPE", "k", "type", "Return the type of the value stored at key")


-- Commands operating on the key space
cmd("KEYS", "p", "keys", "Return all the keys matching a given pattern", 
    { result_handler=keys_iter } )
cmd("RANDOMKEY", "", "randomkey", "Return a random key from the key space")
cmd("RENAME", "nn", "rename", 
    [[Rename the old key in the new one, destroying the newname key if it
    already exists]])

cmd("RENAMENX", "nn", "renamenx", 
    [[Rename the old key in the new one, if the newname key does not
    already exist]])

cmd("DBSIZE", "", "dbsize", "Return the number of keys in the current db")
cmd("EXPIRE", "t", "expire", "Set a time to live in seconds on a key ")


-- Commands operating on lists
cmd("RPUSH", "kv", "rpush", 
    "Append an element to the tail of the List value at key")
cmd("LPUSH", "kv", "lpush",
    "Append an element to the head of the List value at key")
cmd("LLEN", "k", "llen", 
    "Return the length of the List value at key")
cmd("LRANGE", "kse", "lrange", 
    "Return a range of elements from the List at key")
cmd("LTRIM", "kse", "ltrim", 
    "Trim the list at key to the specified range of elements")
cmd("LINDEX", "ki", "lindex", 
    "Return the element at index position from the List at key")
cmd("LSET", "kiv", "lset", 
    "Set a new value as the element at index position of the List at key")
cmd("LREM", "kiv", "lrem", 
    [[Remove the first-N, last-N, or all the elements matching value from
    the List at key]])

cmd("LPOP", "k", "lpop", 
    "Return and remove (atomically) the first element of the List at key")
cmd("RPOP", "k", "rpop", 
    "Return and remove (atomically) the last element of the List at key ")


-- Commands operating on sets
cmd("SADD", "km", "sadd", 
    "Add the specified member to the Set value at key")
cmd("SREM", "km", "srem", 
    "Remove the specified member from the Set value at key")
cmd("SCARD", "k", "scard", 
    "Return the number of elements (the cardinality) of the Set at key")
cmd("SISMEMBER", "km", "sismember", 
    "Test if the specified value is a member of the Set at key", 
    { result_handler=tobool } )
cmd("SINTER", "K", "sinter", 
    "Return the intersection between the Sets stored at key1, key2, ..., keyN")
cmd("SINTERSTORE", "kK", "sinterstore", 
    [[Compute the intersection between the Sets stored at key1, key2, ..., keyN,
    and store the resulting Set at dstkey]])

cmd("SUNION", "K", "sunion", 
    "Return the union between the Sets stored at key1, key2, ..., keyN")
cmd("SUNIONSTORE", "kK", "sunionstore",
    [[Compute the union between the Sets stored at key1, key2, ..., keyN,
    and store the resulting Set at dstkey]])

cmd("SDIFF", "K", "sdiff", 
    [[Return the difference between the Set stored at key1 and all the
    Sets key2, ..., keyN]])

cmd("SDIFFSTORE", "kK", "sdiffstore", 
    [[Compute the difference between the Set key1 and all the Sets 
    key2, ..., keyN, and store the resulting Set at dstkey]])

cmd("SMEMBERS", "k", "smembers", 
    "Return all the members of the Set value at key")


-- Multiple databases handling commands
cmd("SELECT", "i", "select", 
    "Select the DB having the specified index")
cmd("MOVE", "ki", "move", 
    "Move the key from the currently selected DB to the DB having as index dbindex")
cmd("FLUSHDB", "", "flushdb", 
    "Remove all the keys of the currently selected DB")
cmd("FLUSHALL", "", "flushall", 
    "Remove all the keys from all the databases")


-- Sorting
-- TODO: Special case for this? typesig is kpsep .. "[AD]"
--cmd("sort", "SORT", "Key BY pattern LIMIT start end GET pattern ASC|DESC ALPHA Sort a Set or a List accordingly to the specified parameters ")


-- Persistence control commands
cmd("SAVE", "", "save", "Synchronously save the DB on disk")
cmd("BGSAVE", "", "bgsave", "Asynchronously save the DB on disk")
cmd("LASTSAVE", "", "lastsave", 
    "Return the UNIX time stamp of the last successfully saving of the dataset on disk")
cmd("SHUTDOWN", "", "shutdown", 
    "Synchronously save the DB on disk, then shutdown the server ")


-- Remote server control commands
cmd("INFO", "", "info", "Provide information and statistics about the server",
    { result_handler=info_table } )
cmd("MONITOR", "", "monitor", "Dump all the received requests in real time ")


-- Other commands, not in the CommandReference.
cmd("PING", "", "ping", "Ping the database.", 
    { result_handler=function(s) return s == "PONG" end })


---------------------
-- Custom commands --
---------------------

-- Multiple SET, from table.
function Connection:mset(t)
   local sock = self.__socket
   local tosend = {}

   for k,v in pairs(t) do
      tosend[#tosend+1] = fmt("SET %s %d\r\n%s\r\n\r\n",
                              k, len(v), v)
   end

   sock:send(concat(tosend))    --pipeline the whole thing
   local stat, res
   repeat
      stat, res = sock:receive("*l")
      if stat and stat ~= "+OK" then
         error(sub(stat, 2))
      end
   until not stat

   return "OK"
end
