#!/usr/bin/env lua

------------------------------------------------------------------------
-- Copyright (c) 2009, Scott Vokes
-- All rights reserved.
-- 
-- Redistribution and use in source and binary forms, with or without
-- modification, are permitted provided that the following conditions
-- are met:
--     * Redistributions of source code must retain the above copyright
--       notice, this list of conditions and the following disclaimer.
--     * Redistributions in binary form must reproduce the above
--       copyright notice, this list of conditions and the following
--       disclaimer in the documentation and/or other materials
--       provided with the distribution.
--     * Neither the name of the <ORGANIZATION> nor the names of its
--       contributors may be used to endorse or promote products
--       derived from this software without specific prior written
--       permission.
--
-- THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
-- "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
-- LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
-- FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
-- COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
-- INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
-- BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
-- LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
-- CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
-- LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
-- ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
-- POSSIBILITY OF SUCH DAMAGE.
--
------------------------------------------------------------------------
--
-- To connect to a redis server, use:
--     c = redis.connect(host, port)  (defaults to "localhost", 6379)
-- To get a list of supported commands, use:
--     c:help()
--
------------------------------------------------------------------------


------------------
-- Requirements --
------------------

local socket = require "socket"
local fmt, sub = string.format, string.sub

-- module "..."

-- global null sentinel, to distinguish from Lua's nil
NULL = setmetatable( {}, {__tostring = function() return "NULL" end} )


-- Documentation table, filled in by cmd.
local doctable = {}


local Connection = {}           -- prototype

function Connection:help()
   local output = {}
   for key,docpair in pairs(doctable) do
      -- Pretty simple formatting...
      output[#output+1] = fmt("%s (%s):\n  * %s\n", key, docpair[1], docpair[2])
   end
   print(table.concat(output, "\n"))
end


function connect(host, port)
   host = host or "localhost"
   port = port or 6379
   assert(type(host) == "string", "host must be string")
   assert(type(port) == "number" and port > 0 and port < 65536)

   s = socket.connect(host, port)
   if not s then error(fmt("Could not connect to %s port %d.", host, port)) end

   -- Also putting help() in connection namespace, so it's extra-visible.
   local conn = { __socket = s, help = Connection.help }
   setmetatable(conn, { __index = Connection } )

   return conn
end


-- Read a bulk value from a socket.
function bulk(s)
   local val                             -- the value read
   local len = tonumber(s:receive"*l")   -- length response to read
   if len == -1 then return NULL else val = s:receive(len) end

   local nl = assert(s:receive(2) == "\r\n", "Protocol error")
   return val
end


-- Return an iterator for a sequence of bulk values read from a socket.
function bulk_multi(s)
   local ct = tonumber(s:receive"*l")
   local function iter()
      while ct >= 1 do
         assert(s:receive(1) == "$")
         ct = ct - 1
         return bulk(s)
      end
   end
   return iter, ct
end


-- Read and parse a response.
function Connection:receive()
   local t = s:receive(1)

   if t == nil then error("Connection to Redis server was lost.") end

   local action = 
      { ["-"]=function(s) error('Redis: "' .. s:receive"*l" .. '"') end,    -- error
        ["+"]=function(s) return s:receive"*l" end,    -- one line
        ["$"]=bulk,
        ["*"]=bulk_multi,
        [":"]=function(s) return tonumber(s:receive"*l") end  -- integer
     }
   return action[t](self.__socket)
end


-- Send a command and return the response.
function Connection:sendrecv(cmd, handler)
   handler = handler or function(x) return x end
   self.__socket:send(cmd .. "\r\n")
   return handler(self:receive())
end


------------------------
-- Command generation --
------------------------

-- Argument formatters, for sending.
local formatter = {}

function formatter.simple(val) return val end

function formatter.bulk(val)
   return fmt("%s\r\n%s\r\n", string.len(val), val)
end

function formatter.bulklist(list)
   return "TODO"
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

   for t in string.gmatch(type_str, ".") do
      local tpair = assert(types[t], "type not found: " .. t)
      local tk = tpair[1]  --type key
      if tk then type_sig[#type_sig+1] = tk end
   end

   return { table.concat(type_sig, ", "), doc }
end


-- Register a command.
local function cmd(arg_types, name, fname, doc, result_handler) 
   arg_types = arg_types or ""

   local typecheck_fun = typechecker(arg_types)

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

   for k in string.gmatch(ats, ".") do tt[#tt+1] = types[k] end

   return 
   function(...)
      arglist = { ... } or {}   --unprocessed args
      local args = {}           --processed

      if #arglist > #tt then 
         local err = fmt("Too many args, got %d, expected %d", 
                         #arglist, #fmt)
         error(err)
      end

      for i = 1,#arglist do
         local formatter = assert(tt[i][2], "processor not found")
         local checker = assert(tt[i][3], "checker not found")
         local arg = arglist[i]
         if not checker(arg) then
            local err = fmt("Error in argument %d: Got %s, expected %s",
                            i, arg, tt[i][1])
            error(err, 4) -- 4 up stack? or 3?
         end
         args[i] = formatter(arg)

      end
      local argstring = table.concat(args, " ")
      -- print(#args .. " args --> ", argstring)
      return argstring
   end
end


---------------------
-- Result handlers --
---------------------

local function tobool(res)
   if res == 0 then return false
   elseif res == 1 then return true
   else 
      local err = fmt("Expected 0 or 1 (->bool), got %s.", res)
      error(err)
   end
end


-- local function totable
-- local function tolist -> { val, val2, etc. }
-- local function toset -> { key=true, key2=true, etc. }


-- Make a table out of the output from c:info().
local function info_table(s)
   local t = {}
   local gmatch, find, sub = string.gmatch, string.find, string.sub

   for k,v in gmatch(s, "([^:]*):([^\n]*)\n") do
      if k and v then t[k] = v end
   end

   return t
end 

--------------------------
-- High-level interface --
--------------------------

-- key_iter(pattern)
-- # -> count
-- conn[key]  -> val (or { arr val} or { set val }, as the case may be)
-- conn[key] = blah   -> SET or MSET etc., or DEL if nil
--
-- better interface for expire? the time val.
--
-- info -> k:v table

-------------------------
-- Connection handling --
-------------------------
cmd(nil, "quit", "QUIT", "Close the connection")
cmd("k", "auth", "AUTH", "Simple password authentication if enabled")

-----------------------------------------
-- Commands operating on string values --
-----------------------------------------

cmd("kv", "set", "SET", "Set a key to a string value")
cmd("k", "get", "GET", "Return the string value of the key")
cmd("kv", "getset", "GETSET", "Set a key to a string returning the old value of the key")
cmd("K", "mget", "MGET", "Multi-get, return the strings values of the keys")
cmd("kv", "setnx", "SETNX",
    "Set a key to a string value if the key does not exist", tobool)
cmd("k", "incr", "INCR", "Increment the integer value of key")
cmd("ki", "incrby", "INCRBY", "Increment the integer value of key by integer")
cmd("k", "decr", "DECR", "Decrement the integer value of key")
cmd("ki", "decrby", "DECRBY", "Decrement the integer value of key by integer")
cmd("k", "exists", "EXISTS", "Test if a key exists", tobool)
cmd("k", "del", "DEL", "Delete a key")
cmd("k", "type", "TYPE", "Return the type of the value stored at key")


-----------------------------------------
-- Commands operating on the key space --
-----------------------------------------

cmd("p", "keys", "KEYS", "Return all the keys matching a given pattern")
cmd(nil, "randomkey", "RANDOMKEY", "Return a random key from the key space")

-- name -> key?
cmd("nn", "rename", "RENAME", 
    [[Rename the old key in the new one, destroying the newname key if it
already exists]])
cmd("nn", "renamenx", "RENAMENX", 
    [[Rename the old key in the new one, if the newname key does not
already exist]])
cmd(nil, "dbsize", "DBSIZE", "Return the number of keys in the current db")
cmd("t", "expire", "EXPIRE", "Set a time to live in seconds on a key ") -- XXX


---------------------------------
-- Commands operating on lists --
---------------------------------

cmd("kv", "rpush", "RPUSH", 
    "Append an element to the tail of the List value at key")
cmd("kv", "lpush", "LPUSH",
    "Append an element to the head of the List value at key")
cmd("k", "llen", "LLEN", 
    "Return the length of the List value at key")
cmd("kse", "lrange", "LRANGE", 
    "Return a range of elements from the List at key")
cmd("kse", "ltrim", "LTRIM", 
    "Trim the list at key to the specified range of elements")
cmd("ki", "lindex", "LINDEX", 
    "Return the element at index position from the List at key")
cmd("kiv", "lset", "LSET", 
    "Set a new value as the element at index position of the List at key")
cmd("kiv", "lrem", "LREM", 
    [[Remove the first-N, last-N, or all the elements matching value from
the List at key]])
cmd("k", "lpop", "LPOP", 
    "Return and remove (atomically) the first element of the List at key")
cmd("k", "rpop", "RPOP", 
    "Return and remove (atomically) the last element of the List at key ")


--------------------------------
-- Commands operating on sets --
--------------------------------

cmd("km", "sadd", "SADD", 
    "Add the specified member to the Set value at key")
cmd("km", "srem", "SREM", 
    "Remove the specified member from the Set value at key")
cmd("k", "scard", "SCARD", 
    "Return the number of elements (the cardinality) of the Set at key")
cmd("km", "sismember", "SISMEMBER", 
    "Test if the specified value is a member of the Set at key", tobool)
cmd("K", "sinter", "SINTER", 
    "Return the intersection between the Sets stored at key1, key2, ..., keyN")
cmd("kK", "sinterstore", "SINTERSTORE", 
    [[Compute the intersection between the Sets stored at key1, key2, ..., keyN,
and store the resulting Set at dstkey]])
cmd("K", "sunion", "SUNION", 
    "Return the union between the Sets stored at key1, key2, ..., keyN")
cmd("kK", "sunionstore", "SUNIONSTORE",
    [[Compute the union between the Sets stored at key1, key2, ..., keyN,
and store the resulting Set at dstkey]])
cmd("K", "sdiff", "SDIFF", 
    [[Return the difference between the Set stored at key1 and all the
Sets key2, ..., keyN]])
cmd("kK", "sdiffstore", "SDIFFSTORE", 
    [[Compute the difference between the Set key1 and all the Sets 
key2, ..., keyN, and store the resulting Set at dstkey]])
cmd("k", "smembers", "SMEMBERS", 
    "Return all the members of the Set value at key")


------------------------------------------
-- Multiple databases handling commands --
------------------------------------------

cmd("i", "select", "SELECT", 
    "Select the DB having the specified index")
cmd("ki", "move", "MOVE", 
    "Move the key from the currently selected DB to the DB having as index dbindex")
cmd(nil, "flushdb", "FLUSHDB", 
    "Remove all the keys of the currently selected DB")
cmd(nil, "flushall", "FLUSHALL", 
    "Remove all the keys from all the databases")


-------------
-- Sorting --
-------------

-- TODO: Special case for this? typesig is kpsep .. "[AD]"
--cmd("sort", "SORT", "Key BY pattern LIMIT start end GET pattern ASC|DESC ALPHA Sort a Set or a List accordingly to the specified parameters ")


----------------------------------
-- Persistence control commands --
----------------------------------

cmd(nil, "save", "SAVE", "Synchronously save the DB on disk")
cmd(nil, "bgsave", "BGSAVE", "Asynchronously save the DB on disk")
cmd(nil, "lastsave", "LASTSAVE", 
    "Return the UNIX time stamp of the last successfully saving of the dataset on disk")
cmd(nil, "shutdown", "SHUTDOWN", 
    "Synchronously save the DB on disk, then shutdown the server ")


------------------------------------
-- Remote server control commands --
------------------------------------

cmd(nil, "info", "INFO", "Provide information and statistics about the server", info_table)
cmd(nil, "monitor", "MONITOR", "Dump all the received requests in real time ")


--------------------
-- Other commands --
--------------------

cmd(nil, "ping", "PING", "Ping the database.") -- Not in the CommandReference, hmm...
