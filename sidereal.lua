--[=[-------------------------------------------------------------------
Copyright (c) 2009 Scott Vokes <vokes.s@gmail.com>

Permission to use, copy, modify, and/or distribute this software for any
purpose with or without fee is hereby granted, provided that the above
copyright notice and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
--]=]-------------------------------------------------------------------
---To connect to a Redis server, use:
--    <code>c = sidereal.connect(host, port[, pass_hook])</code>
--
-- <p>If a pass hook function is provided, sidereal will call it to
-- defer control whenever reading/writing on a socket would block.</p>
--
-- <p>Normal Redis commands return (nil, error) on error. Mainly,
-- watch for (nil, "closed") if the connection breaks. Redis commands
-- run via proxy() use Lua's error() call, since it isn't possible
-- to do normal error checking on "var = proxy.key".</p>
--
-- <p>All standard Redis commands are prefaced with "R:" below.</p>
--
-- <p>For further usage examples, see the test suite.</p>
------------------------------------------------------------------------

-------------------------
-- Module requirements --
-------------------------

local socket = require "socket"
local coroutine, string, table, io = coroutine, string, table, io

local assert, error, ipairs, pairs, print, setmetatable, type =
   assert, error, ipairs, pairs, print, setmetatable, type
local tonumber, tostring = tonumber, tostring

---Redis library for Lua, with optional non-blocking mode, pipelining,
-- and Lua-style lists & sets.
module("sidereal")

VERSION = 1.0 -- for Redis 1.2.2

local function conststr(k)
   return function () return k end
end


-- global null sentinel, to distinguish Redis's null from Lua's nil
NULL = setmetatable( {}, {__tostring = conststr("[NULL]") })

-- sentinel sent as response to a pipelined command
PIPELINED = setmetatable( {}, {__tostring = conststr("[PIPELINED]") })

-- aliases
local fmt, len, sub, gmatch =
   string.format, string.len, string.sub, string.gmatch
local concat, sort = table.concat, table.sort


DEBUG = false
local function trace(...)
   if DEBUG then print(...) end
end


-- ##############
-- # Connection #
-- ##############

local Sidereal = {}


---Sidereal->string.
function Sidereal:tostring()
   return fmt("sidereal: (%s:%d%s)",
              self.host, self.port, self._pass ~= nil and " async" or "")
end


local ConnMT = { __index = Sidereal, __tostring = Sidereal.tostring }


---Create and return a Sidereal connection object.
-- @param pass_hook If defined, uses non-blocking IO, and this will be
--     run whenever the command would block. (Use this to send a 'pass'
--     message to your coroutine scheduler.)
function connect(host, port, pass_hook)
   host = host or "localhost"
   port = port or 6379
   assert(type(host) == "string", "host must be string")
   assert(type(port) == "number" and port > 0 and port < 65536)

   local conn = { host = host,
                  port = port,
                  _pass = pass_hook,
                  _pipeline = false,
               }
   setmetatable(conn, ConnMT )

   local ok, s = conn:connect()
   if not ok then return nil, s end
   conn._socket = s
   return conn
end


---(Re)Connect to the specified Redis server.
function Sidereal:connect()
   local s, err = socket.connect(self.host, self.port)
   if not s then return nil, err end
   self._socket = s
   if pass_hook then s:settimeout(0) end    --async operation
   s:setoption("tcp-nodelay", true) --disable nagle's algorithm
   if self._dbindex then self:select(self._dbindex) end
   return true, s
end


---Call pass hook (if any).
-- @see connect
function Sidereal:pass()
   local pass = self._pass
   if pass then pass() end
end


---Send a raw string, don't wait for response.
function Sidereal:send(cmd, retry)
   local pass = self.pass

   while true do
      local ok, err = self._socket:send(cmd .. "\r\n")
      if ok then
         trace("SEND:", cmd, retry)
         return true
      elseif err == "closed" then
         if cmd:match("QUIT") then return true
         elseif self._pipeline or retry then
            trace(" -- Reconnect failed (or pipelining)")
            return nil, "closed"
         else
            local ok, err2 = self:connect()
            if ok then
               return self:send(cmd, true)
            else return nil, err2 or "unknown error(2)"
            end
         end
      elseif err ~= "timeout" then return nil, err or "unknown error"
      else
         self:pass()
      end
   end
end


-- forward reference, anchoring _get_response(self) at this scope.
-- The function itself is defined later.
local _get_response


---Send a raw string and (if not pipelining) return the response.
function Sidereal:send_receive(cmd, retry)
   if self._pipeline then
      trace("PIPELINED:", cmd)
      local p = self._pipeline
      p[#p+1] = cmd
      self._pipeline_ct = self._pipeline_ct + 1
      return true, PIPELINED
   else
      local ok, err, rest = self:send(cmd, retry)
      if not ok then return nil, err end
      ok, rest = _get_response(self)
      if ok then
         return ok, rest
      elseif rest == "closed" then
         if cmd:match("QUIT") then return true
         elseif retry then return nil, "closed"
         else
            ok, err = self:connect()
            if ok then
               return self:send_receive(cmd, true)
            else
               return nil, err
            end
         end
      else
         return nil, rest
      end
   end
end


----------------
-- Pipelining --
----------------

---Begin a series of pipelined commands.
-- @usage Use self:send_pipeline() to send them all, and then call
-- self:get_response() once per command. (Successful sends will return
-- sidereal.PIPELINED.)<br>
-- Generally, pipelines should be bookended by MULTI and EXEC.<br>
-- Also, note that (at least as of Redis 1.2.2), Redis will continue
-- queueing pipelined commands until the send is complete, so pipelining
-- a very large sequence of commands can make Redis run out of memory.
-- @see Sidereal:send_pipeline()
-- @see Sidereal:get_response()
function Sidereal:pipeline()
   if self._pipeline then return nil, "Already pipelining" end
   self._pipeline = {}
   self._pipeline_ct = self._pipeline_ct or 0
   return true
end


---Clear the current, unsent pipeline (if any).
function Sidereal:clear_pipeline()
   self._pipeline = nil
   self._pipeline_ct = nil
end


---Send a queue of pipelined commands.
-- @usage Note that if disconnected while sending a pipeline,
--   Sidereal will NOT automatically reconnect. Instead, all of the enqueued
--   commands remain in the pipeline. Be sure to check send_pipeline's result!
function Sidereal:send_pipeline()
   if not self._pipeline then return nil, "Not pipelining" end
   local ok, err = self:send(concat(self._pipeline, "\r\n"))
   if ok then
      self._pipeline = false
   else
      return ok, err
   end
end


---Get the next result from a sent pipeline of commands.
-- @param ignore_ct Don't warn about pipeline counts (i.e., when pipelining manually).
function Sidereal:get_response(ignore_ct)
   if not ignore_ct then
      local ct = self._pipeline_ct
      if not ct or ct < 0 then
         return nil, "Excess pipeline responses"
      elseif ct == 1 then
         self._pipeline_ct = nil
      else
         self._pipeline_ct = ct - 1
      end
   end
   return _get_response(self)
end


---------------
-- Responses --
---------------

-- Read and parse one response, passing on timeout if non-blocking.
function _get_response(self)    --(set as local above)
   local ok, line = self:receive("*l")
   trace("RECV: ", ok, line)
   if not ok then return nil, line end

   return self:handle_response(line)
end


---Receive len bytes, passing on timeout if non-blocking.
function Sidereal:receive(len)
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
         return nil, err
      end
   end
end


-- Read a bulk response.
function Sidereal:bulk_receive(length)
   local buf, rem = {}, length

   while rem > 0 do
      local ok, read = self:receive(rem)
      if not ok then return nil, read end
      buf[#buf+1] = read; rem = rem - read:len()
   end
   local res = concat(buf)
   trace("BULK_RECEIVE: ", res)
   return true, res:sub(1, -3)  --drop the CRLF
end


-- Return an interator for N bulk responses.
function Sidereal:bulk_multi_receive(count)
   local ct = count
   local rs = {}                --results
   trace(" * BULK_MULTI_RECV, ct=", count)

   -- Read and rs all responses, so pipelining works.
   for i=1,ct do
      local ok, read = self:receive("*l")
      if not ok then return nil, read end
      trace("   READLINE:", ok, read)
	  if read:sub(1, 1) == ":" then --integer
		rs[i]  = tonumber(read:sub(2))
	  elseif read:sub(1, 1) == "$" then
	    assert(read:sub(1, 1) == "$", read:sub(1, 1))
	    local length = assert(tonumber(read:sub(2)), "Bad length")
	    if length == -1 then
	        ok, read = true, NULL
	    else
	        ok, read = self:bulk_receive(length + 2)
	    end
	    trace(" -- BULK_READ: ", ok, read)
	    if not ok then return nil, read end
	    rs[i] = read
	  end
   end

   if true then return true, rs end
   local iter = coroutine.wrap(
      function()
         for i=1,ct do
            trace("-- Bulk_multi_val: ", rs[i])
            coroutine.yield(rs[i])
         end
      end)
   return true, iter
end


-- Handle various response types.
function Sidereal:handle_response(line)
   assert(type(line) == "string" and line:len() > 0, "Bad response")
   local r = line:sub(1, 1)

   if r == "+" then             -- +ok
      return true, line:sub(2)
   elseif r == "-" then         -- -error
      return nil, line:match("-ERR (.*)")
   elseif r == ":" then         -- :integer (incl. 0 & 1 for false,true)
      local num = tonumber(line:sub(2))
      return true, num
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
      return nil, "Bad response"
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
             --while simpler, string.format("%s") uses null-terminated strings
             return table.concat{ fmt("%d\r\n", s:len()), s, "\r\n" }
          end,
   bulklist = function(array)
                 if type(array) ~= "table" then array = { array } end
                 return concat(array, " ")
              end,
   table = function(t)
              local buf = {}    --whole message buffer
              for k,v in pairs(t) do
                 v = tostring(v)
                 local b = { "$", tostring(k:len()), "\r\n", k, "\r\n",
                             "$", tostring(v:len()), "\r\n", v, "|r\n" }
                 buf[#buf+1] = concat(b)
              end
              return buf
           end
}


-- type key -> ( description, formatter function, test function )
local types = { k={ "key", formatter.simple, typetest.str },
                d={ "db_index", formatter.simple, typetest.int },
                v={ "value", formatter.bulk, typetest.str },
                K={ "key_list", formatter.bulklist, typetest.strlist },
                V={ "value_list", formatter.bulk, typetest.strlist },
                i={ "integer", formatter.simple, typetest.int },
                f={ "float", formatter.simple, typetest.float },
                p={ "pattern", formatter.simple, typetest.str },
                n={ "name", formatter.simple, typetest.str },
                t={ "time_in_seconds", formatter.simple, typetest.int },
                T={ "table", formatter.table, typetest.table },
                s={ "start_index", formatter.simple, typetest.int },
                e={ "end_index", formatter.simple, typetest.int },
                m={ "member", formatter.bulk, typetest.todo },
             }


local is_vararg = {}
is_vararg[formatter.bulklist] = true

local function gen_arg_funs(funcname, spec)
   if not spec then return function(t) return t end end
   local fs, tts = {}, {}       --formatters, type-testers

   for arg in gmatch(spec, ".") do
      local row = types[arg]
      if not row then error("unmatched: " .. arg) end
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
local function cmd(rfun, arg_types, opts)
   opts = opts or {}
   local name = opts.cmdname or rfun:lower()
   arg_types = arg_types or ""
   local format_args, check = gen_arg_funs(name, arg_types)
   local bulk_send = opts.bulk_send or arg_types == "T"

   Sidereal[name] =
      function(self, ...)
         local raw_args, send = {...}
         if opts.arg_hook then raw_args = opts.arg_hook(raw_args) end
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
            if not arglist then return nil, err end

            if self.DEBUG then check(arglist) end
            local b = { rfun, " ", concat(arglist, " ")}
            send = concat(b)
         end
         
         if opts.pre_hook then send = opts.pre_hook(self, raw_args, send) end
         local ok, res
         if (opts.noreply) then
            ok, res= self:send(send)
         else
            ok, res= self:send_receive(send)
         end
         if ok then
            local ph = opts.post_hook
            if ph then return ph(self, raw_args, res, self) end
            return res
         else
            return nil, res
         end
      end
end


local function num_to_bool(self, r_a, res)
   if self._pipeline or self._multi then return res end
   if type(res) == "number" then return res == 1 end
   return res
end


local function list_to_set(self, r_a, res)
   if self._pipeline or self._multi then return res end
   local set = {}
   for _,k in ipairs(res) do set[k] = true end
   return set
end


local function pair_list_to_set(self, r_a, r)
   if self._pipeline or self._multi then return res end
   local t = {}
   for i=1,#r,2 do t[r[i]] = r[i+1] end
   return t
end


-- ############
-- # Commands #
-- ############

-- Sidereal handling

---R: Close the connection
function Sidereal:quit() end
cmd("QUIT", nil)

---R: Simple password authentication if enabled
function Sidereal:auth(key) end
cmd("AUTH", "k")


-- Commands operating on all the kind of values

---R: Test if a key exists
function Sidereal:exists(key) end
cmd("EXISTS", "k", { post_hook=num_to_bool })

---R: Delete a key
function Sidereal:del(key_list) end
cmd("DEL", "K")

---R: Return the type of the value stored at key
function Sidereal:type(key) end
cmd("TYPE", "k")

---R: Return an iterator listing all keys matching a given pattern
function Sidereal:keys(pattern) end
cmd("KEYS", "p")
   
---R: Return a random key from the key space
function Sidereal:randomkey() end
cmd("RANDOMKEY", nil)

---R: Rename the old key in the new one, destroing the newname key if it
--    already exists
function Sidereal:rename(key, key) end
cmd("RENAME", "kk")

---R: Rename the old key in the new one, if the newname key does not
--    already exist
function Sidereal:renamenx(key, key) end
cmd("RENAMENX", "kk")

---R: Return the number of keys in the current db
function Sidereal:dbsize() end
cmd("DBSIZE", nil)

---R: Set relative a time to live in seconds on a key
function Sidereal:expire(key, time_in_seconds) end
cmd("EXPIRE", "kt")

---R: Set absolute time to live in seconds on a key
function Sidereal:expireat(key, time_in_seconds) end
cmd("EXPIREAT", "kt")

---R: Get the time to live in seconds of a key
function Sidereal:ttl(key) end
cmd("TTL", "k")

---R: Select the DB having the specified index
function Sidereal:select(db_index) end
cmd("SELECT", "d",
    { post_hook =
      function(self, raw_args, res, sdb)
         sdb._dbindex = raw_args[1] --save the DB, for reconnecting
         return res
      end }
 )

---R: Move the key from the currently selected DB to the DB having as
--    index dbindex
function Sidereal:move(key, db_index) end
cmd("MOVE", "kd",
    { post_hook=num_to_bool })

---R: Remove all the keys of the currently selected DB
function Sidereal:flushdb() end
cmd("FLUSHDB", nil)

---R: Remove all the keys from all the databases
function Sidereal:flushall() end
cmd("FLUSHALL", nil)


--Commands operating on string values

---R: Set a key to a string value
function Sidereal:set(key, value) end
cmd("SET", "kv")

---R: Atomically set a key to a string value and expiration time.
function Sidereal:setex(key, value) end
cmd("SETEX", "kiv")

---R: Return the string value of the key
function Sidereal:get(key) end
cmd("GET", "k")

---R: Set a key to a string returning the old value of the key
function Sidereal:getset(key, value) end
cmd("GETSET", "kv")

---R: Multi-get, return the strings values of the keys
function Sidereal:mget(key_list) end
cmd("MGET", "K")

---R: Set a key to a string value if the key does not exist
function Sidereal:setnx(key, value) end
cmd("SETNX", "kv", { post_hook=num_to_bool })

---R: Set a multiple keys to multiple values in a single atomic operation
function Sidereal:mset(table) end
cmd("MSET", "T",
    { post_hook=num_to_bool })

---R: Set a multiple keys to multiple values in a single atomic operation
--    if none of the keys already exist
function Sidereal:msetnx(table) end
cmd("MSETNX", "T",
    { post_hook=num_to_bool })

---R: Increment the integer value of key
function Sidereal:incr(key) end
cmd("INCR", "k")

---R: Increment the integer value of key by integer
function Sidereal:incrby(key, integer) end
cmd("INCRBY", "ki")

---R: Decrement the integer value of key
function Sidereal:decr(key) end
cmd("DECR", "k")

---R: Decrement the integer value of key by integer
function Sidereal:decrby(key, integer) end
cmd("DECRBY", "ki")

---R: Append a value to a key, returning the length.
function Sidereal:append(key, val) end
cmd("APPEND", "kv")

---R: Return a substring of a larger string
function Sidereal:substr(key, from, to) end
cmd("SUBSTR", "kii")


-- Commands operating on lists

---R: Append an element to the tail of the List value at key
function Sidereal:rpush(key, value) end
cmd("RPUSH", "kv")

---R: Append an element to the head of the List value at key
function Sidereal:lpush(key, value) end
cmd("LPUSH", "kv")

---R: Return the length of the List value at key
function Sidereal:llen(key) end
cmd("LLEN", "k")

---R: Return a range of elements from the List at key
function Sidereal:lrange(key, start_index, end_index) end
cmd("LRANGE", "kse")

---R: Trim the list at key to the specified range of elements
function Sidereal:ltrim(key, start_index, end_index) end
cmd("LTRIM", "kse")

---R: Return the element at index position from the List at key
function Sidereal:lindex(key, integer) end
cmd("LINDEX", "ki")

---R: Set a new value as the element at index position of the List at key
function Sidereal:lset(key, integer, value) end
cmd("LSET", "kiv")

---R: Remove the first-N, last-N, or all the elements matching value from
--    the List at key
function Sidereal:lrem(key, integer, value) end
cmd("LREM", "kiv")

---R: Return and atomically remove the first element of the List at key
function Sidereal:lpop(key) end
cmd("LPOP", "k")

---R: Return and atomically remove the last element of the List at key
function Sidereal:rpop(key) end
cmd("RPOP", "k")

---R: Blocking LPOP from any of the lists given, with timeout.
-- @return Returns (listname, val) or (nil, "timeout").
function Sidereal:blpop(...) end
cmd("BLPOP", "Ki",
    { post_hook =
      function(self, raw_args, res)
         if #res == 0 then return nil, "timeout" else return res[1], res[2] end
      end })

---R: Blocking RPOP from any of the lists given, with timeout.
-- @return Returns (listname, val) or (nil, "timeout").
function Sidereal:brpop(...) end
cmd("BRPOP", "Ki",
    { post_hook =
      function(self, raw_args, res)
         if #res == 0 then return nil, "timeout" else return res[1], res[2] end
      end })

---R: Return and atomically remove the last element of the source List
--    stored at _srckey_ and push the same element to the destination
--    List stored at _dstkey_
function Sidereal:rpoplpush(key, key) end
cmd("RPOPLPUSH", "kk")


-- Commands operating on sets

---R: Add the specified member to the Set value at key
function Sidereal:sadd(key, member) end
cmd("SADD", "km",
    { post_hook=num_to_bool })

---R: Remove the specified member from the Set value at key
function Sidereal:srem(key, member) end
cmd("SREM", "km",
    { post_hook=num_to_bool })

---R: Remove and return (pop) a random element from the Set value at key
function Sidereal:spop(key) end
cmd("SPOP", "k")

---R: Move the specified member from one Set to another atomically
function Sidereal:smove(key, key, member) end
cmd("SMOVE", "kkm")

---R: Return the number of elements (the cardinality) of the Set at key
function Sidereal:scard(key) end
cmd("SCARD", "k")

---R: Test if the specified value is a member of the Set at key
function Sidereal:sismember(key, member) end
cmd("SISMEMBER", "km",
    { post_hook=num_to_bool })

---R: Return the intersection between the Sets stored at
--    key1, key2, ..., keyN
function Sidereal:sinter(key_list) end
cmd("SINTER", "K",
    { post_hook=list_to_set })

---R: Compute the intersection between the Sets stored at key1,
--    key2, ..., keyN, and store the resulting Set at dstkey
function Sidereal:sinterstore(key, key_list) end
cmd("SINTERSTORE", "kK")

---R: Return the union between the Sets stored at key1, key2, ..., keyN
function Sidereal:sunion(key_list) end
cmd("SUNION", "K",
    { post_hook=list_to_set })

---R: Compute the union between the Sets stored at key1, key2, ..., keyN,
--    and store the resulting Set at dstkey
function Sidereal:sunionstore(key, key_list) end
cmd("SUNIONSTORE", "kK")

---R: Return the difference between the Set stored at key1 and all the
--    Sets key2, ..., keyN
function Sidereal:sdiff(key_list) end
cmd("SDIFF", "K", { post_hook=list_to_set })

---R: Compute the difference between the Set key1 and all the Sets
--    key2, ..., keyN, and store the resulting Set at dstkey
function Sidereal:sdiffstore(key, key_list) end
cmd("SDIFFSTORE", "kK")

---R: Return all the members of the Set value at key
function Sidereal:smembers(key) end
cmd("SMEMBERS", "k",
    { post_hook=list_to_set })

---R: Return a random member of the Set value at key
function Sidereal:srandmember(key) end
cmd("SRANDMEMBER", "k")


-- Commands operating on sorted sets

---R: Add the specified member to the Sorted Set value at key or update
--    the score if it already exist
function Sidereal:zadd(key, float, member) end
cmd("ZADD", "kfm")

---R: Remove the specified member from the Sorted Set value at key
function Sidereal:zrem(key, member) end
cmd("ZREM", "km")

---R: If the member already exists increment its score by _increment_,
--    otherwise add the member setting _increment_ as score
function Sidereal:zincrby(key, integer, member) end
cmd("ZINCRBY", "kim")

---R: Return a range of elements from the sorted set at key, exactly
--    like ZRANGE, but the sorted set is ordered in traversed in reverse
--    order, from the greatest to the smallest score
function Sidereal:zrevrange(key, start_index, end_index) end
cmd("ZREVRANGE", "kse")

---R: Return all the elements with score >= min and score <= max (a
--    range query) from the sorted set
function Sidereal:zrangebyscore(key, float, float) end
cmd("ZRANGEBYSCORE", "kff",
    { pre_hook=function(self, raw_args, msg)
                  local offset, count = raw_args[4], raw_args[5]
                  if offset and count then
                     offset, count = tonumber(offset), tonumber(count)
                     msg = msg .. fmt(" LIMIT %d %d", offset, count)
                  end
                  return msg
               end})


---R: Remove all elements in the sorted set at key with rank between from and to.
function Sidereal:zremrangebyrank(key, from, to) end
cmd("ZREMRANGEBYRANK", "kii")


---R: Return the cardinality (number of elements) of the sorted set at key
function Sidereal:zcard(key) end
cmd("ZCARD", "k")

---R: Return the score associated with the specified element of the
--    sorted set at key
function Sidereal:zscore(key, value) end
cmd("ZSCORE", "kv")

---R: Remove all the elements with score >= min and score <= max from the
--    sorted set
function Sidereal:zremrangebyscore(key, float, float) end
cmd("ZREMRANGEBYSCORE", "kff")

---R: Return a range of elements from the sorted set at key
function Sidereal:zrange(key, start_index, end_index) end
cmd("ZRANGE", "kse",
    { pre_hook=function(self, raw_args, msg)
                 if raw_args[4] then
                    return msg .. " withscores"
                 else return msg end
              end })

---R: Return count of elements within the score range.
function Sidereal:zcount(key, from, to) end
cmd("ZCOUNT", "kii")

---R: Return rank of member in a ZSET (zero-based).
function Sidereal:zrank(key, member) end
cmd("ZRANK", "kv")

---R: Return rank of member in a ZSET (zero-based), scored highest to lowest.
function Sidereal:zrevrank(key, member) end
cmd("ZREVRANK", "kv")

---R: Store intersection of zsets under keys k1..kN with optional weights w1..wN at dskey.
function Sidereal:zinterstore(dstkey, keycount, ...) end
cmd("ZINTERSTORE", "kiK")

---R: Store union of zsets under keys k1..kN with optional weights w1..wN at dskey.
function Sidereal:zunionstore(dstkey, keycount, ...) end
cmd("ZUNIONSTORE", "kiK")


-- Commands operating on hashes

---R: Set the hash field to the specified value.
function Sidereal:hset(key, field, value) end
cmd("HSET", "kfv")

---R: Get a hash's field.
function Sidereal:hget(key, field) end
cmd("HGET", "kv")

---R: Get all of a hash's fields.
function Sidereal:hgetall(key) end
cmd("HGETALL", "k", { post_hook=pair_list_to_set })


---R: Delete a hash's field.
function Sidereal:hdel(key, field) end
cmd("HDEL", "kv")

---R: Check if a hash has a field.
function Sidereal:hexists(key, field) end
cmd("HEXISTS", "kv")

---R: Return the number of values in a hash.
function Sidereal:hlen(key) end
cmd("HLEN", "k")

---R: Return a hash's keys.
function Sidereal:hkeys(key) end
cmd("HKEYS", "k")

---R: Return a hash's vals.
function Sidereal:hvals(key) end
cmd("HVALS", "k")

---R: Increment the integer value of the hash at key on field with int.
function Sidereal:hincrby(key, field, int) end
cmd("HINCRBY", "kfi")

---R: Atomically get the values for multiple fields in a hash. CURRENTLY BROKEN.
function Sidereal:hmget(key, ...) end
cmd("HMGET", "kV")

---R: Atomically set the values for multiple fields in a hash. CURRENTLY BROKEN.
function Sidereal:hmset(hashkey, table) end
cmd("HMSET", "kT")


-- Persistence control commands

---R: Synchronously save the DB on disk
function Sidereal:save() end
cmd("SAVE", nil)

---R: Asynchronously save the DB on disk
function Sidereal:bgsave() end
cmd("BGSAVE", nil)

---R: Return the UNIX time stamp of the last successfully saving of the
--    dataset on disk
function Sidereal:lastsave() end
cmd("LASTSAVE", nil)

---R: Synchronously save the DB on disk, then shutdown the server
function Sidereal:shutdown() end
cmd("SHUTDOWN", nil)

---R: Rewrite the append only file in background when it gets too big
function Sidereal:bgrewriteaof() end
cmd("BGREWRITEAOF", nil)

---R: Get/Set config options.
-- @usage R:config("get", "pattern")
-- @usage R:config("set", "property", "value")
-- @return Get - table with all properties matching pattern
function Sidereal:config(action, ...) end
cmd("CONFIG", "kv",
    { pre_hook=function(self, raw_args, msg)
                  if raw_args[1] == "get" and raw_args[2] == nil then
                     return "get *" end
                  return msg
               end,
    post_hook=pair_list_to_set }
)


-- Publish/Subscribe
-- See the official doc at http://code.google.com/p/redis/wiki/PublishSubscribe.

---R: Publish message to a channel
function Sidereal:publish(channel, message) end
cmd("PUBLISH", "kv", { noreply=true })

---R: Subscribe to a channel
function Sidereal:suscribe(channel) end
cmd("SUBSCRIBE", "K", { noreply=true })

---R: Unsubscribe from listed channel(s)
function Sidereal:unsubscribe(channel) end
cmd("UNSUBSCRIBE", "K", { noreply=true })

---R: Unsubscribe from all channel
function Sidereal:unsubscribeall(channel) end
cmd("UNSUBSCRIBE", nil, { noreply=true, cmdname="unsubscribeall" })

---R: Subscribe to a pattern
function Sidereal:psuscribe(pattern) end
cmd("PSUBSCRIBE", "p", { noreply=true })

---R: Unsubscribe from a pattern
function Sidereal:punsubscribe(pattern) end
cmd("PUNSUBSCRIBE", "p", { noreply=true })

---R: Unsubscribe from all patterns
function Sidereal:unsubscribe_all() end
cmd("PUNSUBSCRIBE", nil, { noreply=true, cmdname="punsubscribeall"  })

---R: Listen for subscription broadcasts.
-- Returns a 3-tuple of { operation, channel_id, message }, such as
-- {"subscribe", "channel_name", 3} (3 being the number of current subscriptions).
-- Operations include "subscribe", "unsubscribe", "psubscribe", "punsubscribe",
-- "message", and "pmessage".
-- Messages recieved by listening to patterns come as a 4-tuple of
-- {operation, pattern, actual channel name, message}.
function Sidereal:listen()
	local ok, rest = _get_response(self)
	return ok, rest
end


---R: Begin an atomic transaction of multiple commands
function Sidereal:multi() end
cmd("MULTI", nil, { pre_hook=function(self) self._multi = true end })

---R: Complete the current transaction
function Sidereal:exec() end
cmd("EXEC", nil)

---R: Abort the current transaction
function Sidereal:discard() end
cmd("DISCARD", nil)

--[[
-- Not available in 2.0.2...
---R: Set key(s) whose modification should abort the current transaction
function Sidereal:watch(...) end
cmd("WATCH", "K")

---R: Clear the set of WATCHed keys
function Sidereal:unwatch() end
cmd("UNWATCH", nil)
--]]

-- Remote server control commands

---R: Provide information and statistics about the server
function Sidereal:info() end
cmd("INFO", nil,
    { post_hook =
      function(self, raw_args, res)
         if raw_args[1] then return res end --"raw" flag
         local t = {}
         for k,v in gmatch(res, "(.-):(.-)\r\n") do
            if v:match("^%d+$") then v = tonumber(v) end
            t[k] = v
         end
         return t
      end })

---R: Dump all the received requests in real time
function Sidereal:monitor() end
cmd("MONITOR", nil)

---R: Set/clear replication of another server.
function Sidereal:slaveof(key, integer) end
cmd("SLAVEOF", "ki",
    {arg_hook = function(args)
                   if #args == 0 then return {"no", "one"} end
                   return args
                end})


-- These three are not in the reference, but were in the TCL test suite.

---R: Ping the database
function Sidereal:ping() end
cmd("PING", nil)

---R: Set debug flag (undocumented, in official test suite)
function Sidereal:debug() end
cmd("DEBUG", nil)

---R: Force reload of data(?) (undocumented, in official test suite)
function Sidereal:reload() end
cmd("RELOAD", nil)


local known_sort_opts = { by=true, start=true, count=true, get=true,
                          asc=true, desc=true, alpha=true, store=true }

---R: Sort the elements contained in the List, Set, or Sorted Set value at key.
-- @usage e.g. r:sort("key", { start=10, count=25, alpha=true })
-- @param options by, start and count, get, asc or desc or alpha, store
function Sidereal:sort(key, options)
   local t = options or {}
   local by = t.by
   local start, count = t.start, t.count    --"start" could instead be "offset"
   local get = t.get
   local asc, desc, alpha = t.asc, t.desc, t.alpha --ASC is default
   local store = t.store
   for k in pairs(t) do
      if not known_sort_opts[k] then
         return nil, "Bad sort option: " .. tostring(k)
      end
   end

   assert(not(asc and desc), "ASC and DESC are mutually exclusive")
   local b = {}

   b[1] = fmt("SORT %s", tostring(key))
   if by then b[#b+1] = fmt("BY %s", by) end
   if start and count then b[#b+1] = fmt("LIMIT %d %d", start, count) end
   if get then b[#b+1] = fmt("GET %s", get) end
   if asc then b[#b+1] = "ASC" elseif desc then b[#b+1] = "DESC" end
   if alpha then b[#b+1] = "ALPHA" end
   if store then b[#b+1] = fmt("STORE %s", store) end

   trace("-- SORTING:", concat(b, " "))

   local ok, res = self:send_receive(concat(b, " "))
   if ok then return res else return nil, res end
end


-- #########
-- # Proxy #
-- #########

---Return a table that can be used as a proxy (via __index and __newindex).
-- Strings, lists, sets, and zsets are supported.
-- @usage p.key = value
-- @usage p.key = { "a", "LIST", "of", "keys" }
-- @usage p.key = { a=true, lua=true, style=true, SET=true }
-- @usage p.key = { a=10, weighted=23, ZSET=47 }
-- @usage result = p.key  --which is converted back to the above
function Sidereal:proxy()
   local function index(_, key)
      local val, err = self:get(key)
      if val then return val
      elseif err and string.match(err, "wrong kind of value") then
         local t, err2 = self:type(key)
         if not t then return nil, err2 end
         if t == "none" then
            return NULL
         elseif t == "list" then
            return self:lrange(key, 0, -1)
         elseif t == "set" then
            return self:smembers(key)
         elseif t == "zset" then
            return self:zrange(key, 0, -1)
         end
      end
      return nil, err
   end

   local function set_list(key, list)
      for i,v in ipairs(list) do
         local ok, err = self:rpush(key, v)
         if not ok then return nil, err end
      end
      return true
   end

   local function set_set(key, set)
      for v in pairs(set) do
         local ok, err = self:sadd(key, v)
         if not ok then return nil, err end
      end
      return true
   end

   local function set_zset(key, zset)
      for v,wt in pairs(zset) do
         local ok, err = self:zadd(key, wt, v)
         if not ok then return nil, err end
      end
      return true
   end

   local function set_table(key, val)
      local t_type
      local bool_v, weight_v, int_k = true, true, true
      for k,v in pairs(val) do
         if type(k) ~= "number" then int_k = false end
         if type(v) ~= "boolean" then bool_v = false end
         if type(v) ~= "number" then weight_v = false end
         --print(bool_v, weight_v, int_k, k, v, type(k), type(v))

         if bool_v and not weight_k and not int_k then t_type="set"; break
         elseif not bool_v and weight_v and not int_k then t_type="zset"; break
         elseif not bool_v and not weight_k and int_k then t_type="list"; break
         elseif not bool_v and not weight_k and not int_k then t_type="error"; break
         end
      end
      if t_type == "list" then return set_list(key, val)
      elseif t_type == "set" then return set_set(key, val)
      elseif t_type == "zset" then return set_zset(key, val)
      end
      return nil, "Bad table sent to proxy:__newindex"
   end

   local function newindex(_, key, val)
      local val_t, ok, err = type(val)
      if val_t == "table" then
         ok, err = set_table(key, val)
      elseif val_t == "string" or val_t == "number" or val_t == "boolean" then
         ok, err = self:set(key, val)
      elseif val_t == "nil" then
         ok, err = self:del(key)
      else
         ok, err = nil, "Bad Lua value type: " .. val_t
      end
      -- Use Lua's error() because you can't wrap (x=y) in asserts, etc.
      if not ok then error(err) end
   end

   local function pts(s)
      return tostring(s) .. "(proxy)"
   end

   local pr = setmetatable({}, { __index=index,__newindex=newindex,
                                 __tostring=pts})
   return pr
end
