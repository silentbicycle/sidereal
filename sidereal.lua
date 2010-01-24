------------------------------------------------------------------------
-- Copyright (c) 2009 Scott Vokes <vokes.s@gmail.com>
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
-- To connect to a Redis server, use:
--     c = sidereal.connect(host, port, [pass_hook])
--
-- If a pass hook function is provided, sidereal is non-blocking.
--
------------------------------------------------------------------------


------------------------------------------------------------------------
-- TODO
-- * finish converting test suite
-- * LuaDoc stubs for generated commands?
-- * foundation for consistent hashing?
-- * profile and tune
-- * handle auto-reconnecting
-- * better high-level handling of sets (as a { key=true } table
-- * maybe create a simple proxy object w/ __index and __newindex
------------------------------------------------------------------------


-------------------------
-- Module requirements --
-------------------------

local socket = require "socket"
local coroutine, string, table = coroutine, string, table

local assert, error, ipairs, pairs, print, setmetatable, type =
   assert, error, ipairs, pairs, print, setmetatable, type
local tonumber, tostring = tonumber, tostring

module(...)

-- global null sentinel, to distinguish Redis's null from Lua's nil
NULL = setmetatable( {}, {__tostring = function() return "[NULL]" end} )


-- aliases
local fmt, len, sub, gmatch =
   string.format, string.len, string.sub, string.gmatch
local concat, sort = table.concat, table.sort


DEBUG = false
local function trace(...)
   if DEBUG then print(...) end
end


----------------
-- Connection --
----------------

local Connection = {}           -- prototype
local ConnMT = { __index = Connection, __string = Connection.tostring }


---Create and return a connection object.
-- @param pass_hook If defined, use non-blocking IO, and run it to defer control.
-- (use to send a 'pass' message to your coroutine scheduler.)
function connect(host, port, pass_hook)
   host = host or "localhost"
   port = port or 6379
   assert(type(host) == "string", "host must be string")
   assert(type(port) == "number" and port > 0 and port < 65536)

   local s = socket.connect(host, port)
   if pass_hook then s:settimeout(0) end    --async operation
   if not s then 
      error(fmt("Could not connect to %s port %d.", host, port)) 
   end

   local conn = { _socket = s, 
                  host = host,
                  port = port,
                  _pass = pass_hook,
               }
   return setmetatable(conn, ConnMT )
end


---Call pass hook.
-- @usage Called to yield to caller, for non-blocking usage.
function Connection:pass()
   local pass = self._pass
   if pass then pass() end
end


---Send a command, don't wait for response.
function Connection:send(cmd)
   local pass = self.pass

   while true do
      local ok, err = self._socket:send(cmd .. "\r\n")
      if ok then
         trace("SEND(only):", cmd)
         return true
      elseif err ~= "timeout" then return false, err
      else
         self:pass()
      end
   end   
end


---Send a command and return the response.
function Connection:sendrecv(cmd, to_bool)
   trace("SEND:", cmd)
   self._socket:send(cmd .. "\r\n")
   return self:receive_line(to_bool)
end


---------------
-- Responses --
---------------

---Read and handle response, passing if necessary.
function Connection:receive_line(to_bool)
   local ok, line = self:receive("*l")
   trace("RECV: ", ok, line)
   if not ok then return false, line end
   
   return self:handle_response(line, to_bool)
end


---Receive, passing if necessary.
function Connection:receive(len)
   -- TODO rather than using socket:receive, receive bulk and save unread?
   --      wait until after the test suite is done.
   local s, pass = self._socket, self.pass
   while true do
      local data, err, rest = s:receive(len)
      local read = data or rest

      if read and read ~= "" then
         return true, read
      elseif err == "timeout" then
         self:pass()
      else
         return false, err
      end
   end
end


---Read a bulk response.
function Connection:bulk_receive(length)
   local buf, rem = {}, length

   while rem > 0 do
      local ok, read = self:receive(rem)
      if not ok then return false, read end
      buf[#buf+1] = read; rem = rem - read:len()
   end
   local res = concat(buf)
   trace("BULK_RECEIVE: ", res)
   return true, res:sub(1, -3)  --drop the CRLF
end


---Return an interator for N bulk responses.
function Connection:bulk_multi_receive(count)
   local ct = count
   local queue = {}
   trace(" * BULK_MULTI_RECV, ct=", count)

   -- Read and queue all responses, so pipelining works.
   for i=1,ct do
      local ok, read = self:receive("*l")
      if not ok then return false, read end
      trace("   READLINE:", ok, read)
      assert(read:sub(1, 1) == "$", read:sub(1, 1))
      local length = assert(tonumber(read:sub(2)), "Bad length")
      if length == -1 then
         ok, read = true, NULL
      else
         ok, read = self:bulk_receive(length + 2)
      end
      trace(" -- BULK_READ: ", ok, read)
      if not ok then return false, read end
      queue[i] = read
   end


   local iter = coroutine.wrap(
      function()
         for i=1,ct do
            trace("-- Bulk_multi_val: ", queue[i])
            coroutine.yield(queue[i])
         end
      end)
   return true, iter
end


---Handle response lines.
function Connection:handle_response(line, to_bool)
   assert(type(line) == "string" and line:len() > 0, "Bad response")
   local r = line:sub(1, 1)

   if r == "+" then             -- +ok
      return true, line:sub(2)
   elseif r == "-" then         -- -error
      return false, line:match("-ERR (.*)")
   elseif r == ":" then         -- :integer (incl. 0 & 1 for false,true)
      local num = tonumber(line:sub(2))
      if to_bool then
         return true, num == 1 
      else
         return true, num
      end
   elseif r == "$" then         -- $4\r\nbulk\r\n
      local len = assert(tonumber(line:sub(2)), "Bad length")
      if len == -1 then
         return true, NULL
      elseif len == 0 then
         assert(self:receive(2), "No CRLF following $0")
         return true, ""
      else
         local ok, data = self:bulk_receive(len + 2)
         return ok, data
      end
   elseif r == "*" then         -- bulk-multi (e.g. *3\r\n(three bulk) )
      local count = assert(tonumber(line:sub(2)), "Bad count")
      return self:bulk_multi_receive(count)
   else
      return false, "Bad response"
   end
end


------------------------
-- Command generation --
------------------------

local typetest = {
   str = function(x) return type(x) == "string" end,
   int = function(x) return type(x) == "number" and math.floor(x) == x end,
   float = function(x) return type(x) == "number" end,
   table = function(x) return type(x) == "table" end,
   strlist = function(x)
                if type(x) ~= "table" then return false end
                for k,v in pairs(x) do
                   if type(k) ~= "number" and type(v) ~= "string" then return false end
                end
                return true
             end
}


local formatter = {
   simple = function(x) return tostring(x) end,
   bulk = function(x)
             local s = tostring(x)
             return fmt("%d\r\n%s\r\n", s:len(), s)
          end,
   bulklist = function(array)
                 if type(array) ~= "table" then array = { array } end
                 return concat(array, " ")
              end,
   table = function(t)
              local b = {}
              for k,v in pairs(t) do
                 v = tostring(v)
                 b[#b+1] = fmt("$%d\r\n%s\r\n$%d\r\n%s\r\n",
                               k:len(), k, v:len(), v)
              end
              return b
           end
}


-- type key -> ( description, formatter function, test function )
local types = { k={ "key", formatter.simple, typetest.str },
                d={ "db", formatter.simple, typetest.int },
                v={ "value", formatter.bulk, typetest.str },
                K={ "key list", formatter.bulklist, typetest.strlist },
                i={ "integer", formatter.simple, typetest.int },
                f={ "float", formatter.simple, typetest.float },
                p={ "pattern", formatter.simple, typetest.str },
                n={ "name", formatter.simple, typetest.str },
                t={ "time", formatter.simple, typetest.int },
                T={ "table", formatter.table, typetest.table },
                s={ "start", formatter.simple, typetest.int },
                e={ "end", formatter.simple, typetest.int },
                m={ "member", formatter.bulk, typetest.todo },      -- FIXME, key???
             }


local is_vararg = {}
-- uses_bulk_args[formatter.table] = true
is_vararg[formatter.bulklist] = true

local function gen_arg_funs(funcname, spec)
   if not spec then return function(t) return t end end
   local fs, tts = {}, {}       --formatters, type-testers

   for arg in gmatch(spec, ".") do
      local row = types[arg]
      if not row then error("unmatched ", arg) end
      fs[#fs+1], tts[#tts+1] = row[2], row[3]
   end

   local check = function(args)
         for i=1,#tts do if not tts[i](args[i]) then return false end end
         return true
      end
   
   local format = function(t)
         local args = {}
         for i=1,#fs do
            if is_vararg[fs[i]] then
               for rest=i,#t do args[rest] = tostring(t[rest]) end
               break
            end
            args[i] = fs[i](t[i])
         end
         if #args < #fs then error("Not enough arguments") end
         return args
      end
   return format, check
end


-- Register a command.
local function cmd(rfun, arg_types, doc, opts) 
   opts = opts or {}
   local name = rfun:lower()
   arg_types = arg_types or ""
   local format_args, check = gen_arg_funs(name, arg_types)
   local bulk_send = opts.bulk_send or arg_types == "T"

   Connection[name] = 
      function(self, ...) 
         local raw_args, send = {...}
         if opts.arg_hook then raw_args = arg_hook(raw_args) end
         local arglist, err = format_args(raw_args)
         if bulk_send then
            local b = {}
            arglist = arglist[1]
            local arg_ct = 2*(#arglist)
            b[1] = fmt("*%d\r\n$%d\r\n%s\r\n",
                       arg_ct + 1, rfun:len(), rfun)
            for _,arg in ipairs(arglist) do b[#b+1] = arg end
            send = concat(b)
         else
            if not arglist then return false, err end
            
            if self.DEBUG then check(arglist) end
            send = fmt("%s %s", rfun, concat(arglist, " "))
         end

         if opts.pre_hook then send = opts.pre_hook(raw_args, send) end
         local ok, res = self:sendrecv(send, opts.to_bool)
         if ok then
            if opts.post_hook then return opts.post_hook(raw_args, res) end
            return res
         else
            return false, res
         end
      end
end


--------------
-- Commands --
--------------

-- Connection handling
cmd("QUIT", nil, "Close the connection")
cmd("AUTH", "k", "Simple password authentication if enabled")

-- Commands operating on all the kind of values
cmd("EXISTS", "k", "Test if a key exists", { to_bool=true })
cmd("DEL", "K", "Delete a key")
cmd("TYPE", "k", "Return the type of the value stored at key")
cmd("KEYS", "p", "Return an iterator listing all keys matching a given pattern",
    { post_hook =
      function(raw_args, res)
         if raw_args[2] then return res end
         return string.gmatch(res, "([^ ]+)")
      end})
cmd("RANDOMKEY", nil, "Return a random key from the key space")
cmd("RENAME", "kk", "Rename the old key in the new one, destroing the newname key if it already exists")
cmd("RENAMENX", "kk", "Rename the old key in the new one, if the newname key does not already exist")
cmd("DBSIZE", nil, "Return the number of keys in the current db")
cmd("EXPIRE", "kt", "Set a time to live in seconds on a key")   --???
cmd("EXPIREAT", "kt", "Set a time to live in seconds on a key") --???
cmd("TTL", "k", "Get the time to live in seconds of a key")
cmd("SELECT", "d", "Select the DB having the specified index")
cmd("MOVE", "kd", "Move the key from the currently selected DB to the DB having as index dbindex",
    { to_bool=true })
cmd("FLUSHDB", nil, "Remove all the keys of the currently selected DB")
cmd("FLUSHALL", nil, "Remove all the keys from all the databases")

--Commands operating on string values
cmd("SET", "kv", "Set a key to a string value")
cmd("GET", "k", "Return the string value of the key")
cmd("GETSET", "kv", "Set a key to a string returning the old value of the key")
cmd("MGET", "K", "Multi-get, return the strings values of the keys")
cmd("SETNX", "kv", "Set a key to a string value if the key does not exist", { to_bool=true })
cmd("MSET", "T", "Set a multiple keys to multiple values in a single atomic operation",
    { to_bool=true })
cmd("MSETNX", "T", "Set a multiple keys to multiple values in a single atomic operation if none of the keys already exist",
    { to_bool=true })
cmd("INCR", "k", "Increment the integer value of key")
cmd("INCRBY", "ki", "Increment the integer value of key by integer")
cmd("DECR", "k", "Decrement the integer value of key")
cmd("DECRBY", "ki", "Decrement the integer value of key by integer")

-- Commands operating on lists
cmd("RPUSH", "kv", "Append an element to the tail of the List value at key")
cmd("LPUSH", "kv", "Append an element to the head of the List value at key")
cmd("LLEN", "k", "Return the length of the List value at key")
cmd("LRANGE", "kse", "Return a range of elements from the List at key")
cmd("LTRIM", "kse", "Trim the list at key to the specified range of elements")
cmd("LINDEX", "ki", "Return the element at index position from the List at key")
cmd("LSET", "kiv", "Set a new value as the element at index position of the List at key")
cmd("LREM", "kiv", "Remove the first-N, last-N, or all the elements matching value from the List at key")
cmd("LPOP", "k", "Return and remove (atomically) the first element of the List at key")
cmd("RPOP", "k", "Return and remove (atomically) the last element of the List at key")
cmd("RPOPLPUSH", "kk", "Return and remove (atomically) the last element of the source List stored at _srckey_ and push the same element to the destination List stored at _dstkey_")

-- Commands operating on sets
cmd("SADD", "km", "Add the specified member to the Set value at key",
    { to_bool=true })
cmd("SREM", "km", "Remove the specified member from the Set value at key",
    { to_bool=true })
cmd("SPOP", "k", "Remove and return (pop) a random element from the Set value at key")
cmd("SMOVE", "kkm", "Move the specified member from one Set to another atomically")
cmd("SCARD", "k", "Return the number of elements (the cardinality) of the Set at key")
cmd("SISMEMBER", "km", "Test if the specified value is a member of the Set at key",
    { to_bool=true })
cmd("SINTER", "K", "Return the intersection between the Sets stored at key1, key2, ..., keyN")
cmd("SINTERSTORE", "kK", "Compute the intersection between the Sets stored at key1, key2, ..., keyN, and store the resulting Set at dstkey")
cmd("SUNION", "K", "Return the union between the Sets stored at key1, key2, ..., keyN")
cmd("SUNIONSTORE", "kK", "Compute the union between the Sets stored at key1, key2, ..., keyN, and store the resulting Set at dstkey")
cmd("SDIFF", "K", "Return the difference between the Set stored at key1 and all the Sets key2, ..., keyN")
cmd("SDIFFSTORE", "kK", "Compute the difference between the Set key1 and all the Sets key2, ..., keyN, and store the resulting Set at dstkey")
cmd("SMEMBERS", "k", "Return all the members of the Set value at key")
cmd("SRANDMEMBER", "k", "Return a random member of the Set value at key")


-- Commands operating on sorted sets
cmd("ZADD", "kfm", "Add the specified member to the Sorted Set value at key or update the score if it already exist")
cmd("ZREM", "km", "Remove the specified member from the Sorted Set value at key")
cmd("ZINCRBY", "kim", "If the member already exists increment its score by _increment_, otherwise add the member setting _increment_ as score")
cmd("ZREVRANGE", "kse", "Return a range of elements from the sorted set at key, exactly like ZRANGE, but the sorted set is ordered in traversed in reverse order, from the greatest to the smallest score")
cmd("ZRANGEBYSCORE", "kff", "Return all the elements with score >= min and score <= max (a range query) from the sorted set",
    { pre_hook=function(raw_args, msg)
                  local offset, count = raw_args[4], raw_args[5]
                  if offset and count then
                     offset, count = tonumber(offset), tonumber(count)
                     msg = msg .. fmt(" LIMIT %d %d", offset, count)
                  end
                  return msg
               end})
cmd("ZCARD", "k", "Return the cardinality (number of elements) of the sorted set at key")
cmd("ZSCORE", "kv", "Return the score associated with the specified element of the sorted set at key")
cmd("ZREMRANGEBYSCORE", "kff", "Remove all the elements with score >= min and score <= max from the sorted set")
cmd("ZRANGE", "kse", "Return a range of elements from the sorted set at key",
    { pre_hook=function(raw_args, msg)
                 if raw_args[4] then
                    return msg .. " withscores"
                 else return msg end
              end })

-- Persistence control commands
cmd("SAVE", nil, "Synchronously save the DB on disk")
cmd("BGSAVE", nil, "Asynchronously save the DB on disk")
cmd("LASTSAVE", nil, "Return the UNIX time stamp of the last successfully saving of the dataset on disk")
cmd("SHUTDOWN", nil, "Synchronously save the DB on disk, then shutdown the server")
cmd("BGREWRITEAOF", nil, "Rewrite the append only file in background when it gets too big")

-- Remote server control commands
cmd("INFO", nil, "Provide information and statistics about the server",
    { post_hook =
      function(raw_args, res)
         if raw_args[1] then return res end --"raw" flag
         local t = {}
         for k,v in gmatch(res, "(.-):(.-)\r\n") do
            if v:match("^%d+$") then v = tonumber(v) end
            t[k] = v
         end
         return t
      end })
cmd("MONITOR", nil, "Dump all the received requests in real time")
cmd("SLAVEOF", "ki", "Set/clear replication of another server.",
    {arg_hook = function(args)
                   if #args == 0 then return {"no", "one"} end
                   return args
                end})

-- These three are not in the reference, but were in the TCL test suite.
cmd("PING", nil, "Ping the database")
cmd("DEBUG", nil, "???")
cmd("RELOAD", nil, "???")


---Sort the elements contained in the List, Set, or Sorted Set value at key.
-- SORT key [BY pattern] [LIMIT start count] [GET pattern] [ASC|DESC] [ALPHA] [STORE dstkey]
-- use e.g. r:sort("key", { start=10, count=25, alpha=true })
function Connection:sort(key, t)
   local by = t.by
   local start, count = t.start, t.count
   local pattern = t.by
   local asc, desc, alpha = t.asc, t.desc, t.alpha --ASC is default
   local dstkey = t.dstkey

   assert(not(asc and desc), "ASC and DESC are mutually exclusive")
   local b = {}

   b[1] = fmt("SORT %s", tostring(key))
   if pattern then b[#b+1] = fmt("BY %s", pattern) end
   if start and count then b[#b+1] = fmt("LIMIT %d %d", start, count) end
   if asc then b[#b+1] = "ASC" elseif desc then b[#b+1] = "DESC" end
   if alpha then b[#b+1] = "ALPHA" end
   if dstkey then b[#b+1] = fmt("STORE %s", dstkey) end
   
   return self:sendrecv(concat(b, " "), result_handler) 
end
