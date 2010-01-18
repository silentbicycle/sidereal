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
-- To connect to a Redis server, use:
--     c = sidereal.connect(host, port, [pass_hook])
--
-- If a pass hook function is provided, sidereal is non-blocking.
--
------------------------------------------------------------------------


-------------------------
-- Module requirements --
-------------------------

local socket = require "socket"
local string, table = string, table

local assert, error, ipairs, pairs, print, setmetatable, type =
   assert, error, ipairs, pairs, print, setmetatable, type
local tonumber, tostring = tonumber, tostring

module(...)

-- global null sentinel, to distinguish from Lua's nil
NULL = setmetatable( {}, {__tostring = function() return "[NULL]" end} )


-- aliases
local fmt, len, sub, gmatch =
   string.format, string.len, string.sub, string.gmatch
local concat, sort = table.concat, table.sort


----------------
-- Connection --
----------------

local Connection = {}           -- prototype
local ConnMT = { __index = Connection, __string = Connection.tostring }


---Create and return a connection object.
-- @param pass_hook If defined, use non-blocking IO, and run it to defer control.
-- (use to send a 'pass' message to a coroutine scheduler.)
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

   -- Also putting help() in connection namespace, so it's extra-visible.
   local conn = { _socket = s, 
                  host = host,
                  port = port,
                  _pass = pass_hook,
                  help = Connection.help
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
      if ok then return true
      elseif err ~= "timeout" then return false, err
      else
         self:pass()
      end
   end   
end


---Send a command and return the response.
function Connection:sendrecv(cmd, handler)
   self._socket:send(cmd .. "\r\n")
   local res = self:receive()
   if handler then
      return handler(res)
   else
      return res
   end 
end


---------------
-- Responses --
---------------

---Read and handle response, passing if necessary.
function Connection:receive_line()
   local ok, line = self:receive("*l")
   if not ok then return false, line end
   
   return self:handle_response(line)
end


---Receive, passing if necessary.
function Connection:receive(len)
   -- TODO rather than using socket:receive, should receive bulk and save unread.
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
      local ok, read = self:receive(rem + 2) -- +2 for CRLF
      if not ok then return false, read end

      buf[#buf+1] = read
      rem = rem - read:len()
   end
   return true, concat(buf)
end


---Return an interator for N bulk responses.
function Connection:bulk_multi_receive(count)
   local ct = count

   local iter = coroutine.wrap(
      function()
         while ct > 0 do
            local ok, read = self:receive("*l")
            if not ok then return false, read end

            local length = assert(tonumber(read), "Bad length")
            ok, read = self:bulk_receive(length)
            if ok then
               coroutine.yield(true, read)
            else
               return false, read
            end
         end
      end)
   return iter
end


---Handle response lines.
function Connection:handle_response(line, s, pass, to_bool)
   assert(type(line) == "string" and line:len() > 0, "Bad response")
   local r = line:sub(1, 2)

   if r == "+" then             -- +ok
      return true, line:sub(2)
   elseif r == "-" then         -- -error
      return false, line:sub(2)
   elseif r == ":" then         -- :integer (incl. 0 & 1 for false,true)
      local num = tonumber(line:sub(2))
      return true and (to_bool and num == 1 or num)
   elseif r == "$" then         -- $4\r\nbulk\r\n
      local len = assert(tonumber(line:sub(2)), "Bad length")
      if len == -1 then
         return true, NULL
      else
         local ok, data = self:receive(len)
         return ok, data
      end
   elseif r == "*" then         -- bulk-multi (e.g. *3\r\n(three bulk) )
      local count = assert(tonumber(line:sub(2)), "Bad count")
      return true, self:bulk_multi_receive(count)
   else
      return false, "Bad response"
   end
end


--------------
-- Commands --
--------------

local typetest = {
   str = function(x) return type(x) == "string" end,
   int = function(x) return type(x) == "number" and math.floor(x) == x end,
   float = function(x) return type(x) == "number" end,
   table = function(x) return type(x) == "table" end,
   strlist = function(x)
                if type(x) ~= "table" then return false end
                for k,v in pairs(x) do
                   if type(k) ~= "number" or type(v) ~= "string" then return false end
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
                 local ct, buf = #array, {}
                 for _,val in ipairs(array) do
                    buf[#buf+1] = fmt("%d\r\n%s\r\n", val:len(), val)
                 end
                 return fmt("%d\r\n%s", ct, concat(buf))
              end,
   table = function(t)
              local buf = {}
              for k,v in pairs(t) do
                 buf[#buf+1] = fmt("%s %s", k, v)
              end
              return concat(buf, " ")
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

local function gen_arg_hook(funcname, spec)
   if not spec then
      return function(t) return t end
   end

   local fs, tts = {}, {}

   for arg in gmatch(spec, ".") do
      --print(funcname, arg)
      local row = types[arg]
      if row then
         local f, tt = row[2], row[3]
         fs[#fs+1] = f
         tts[#tts+1] = tt
      else
         print("TODO Handle ", arg)
      end
   end

   local check = function(args)
                    for i=1,#tts do
                       if not tts[i](args[i]) then return false end
                    end
                    return true
                 end

   return function(t)
             --print("Called " .. funcname, #fs)
             local args = {}
             for i,v in ipairs(t) do
                --print("*", i, v)
                args[i] = fs[i](v)
             end
             return args, check
          end
end


-- Register a command.
local function cmd(rfun, arg_types, name, doc, opts) 
   opts = opts or {}
   arg_types = arg_types or ""
   local result_handler = opts.result_handler    -- optional

   local format_args, check = gen_arg_hook(name, arg_types)

   Connection[name] = 
      function(self, ...) 
         local arglist = format_args({...})

         if self.DEBUG then check(arglist) end
         local send = fmt("%s %s", rfun, concat(arglist, " "))
         return self:sendrecv(send, result_handler) 
      end

   --doctable[name] = docstring
end

-- Connection handling
cmd("QUIT", nil, "quit")
cmd("AUTH", "k", "auth")

-- Commands operating on all the kind of values
cmd("EXISTS", "k", "exists", true)
cmd("DEL", "k", "del")
cmd("TYPE", "k", "type")
cmd("KEYS", "p", "keys")
cmd("RANDOMKEY", nil, "randomkey")
cmd("RENAME", "kk", "rename")
cmd("RENAMENX", "kk", "renamenx")
cmd("DBSIZE", nil, "dbsize")
cmd("EXPIRE", "kt", "expire")
cmd("EXPIREAT", "kt", "expireat")
cmd("TTL", "k", "ttl")
cmd("SELECT", "d", "select")
cmd("MOVE", "kd", "move", true)
cmd("FLUSHDB", nil, "flushdb")
cmd("FLUSHALL", nil, "flushall")

--Commands operating on string values
cmd("SET", "kv", "set")
cmd("GET", "k", "get")
cmd("GETSET", "kv", "getset")
cmd("MGET", "K", "mget")
cmd("SETNX", "kv", "setnx", true)
cmd("MSET", "T", "mset")        --FIXME
cmd("MSETNX", "T", "msetnx", true)
cmd("INCR", "k", "incr")
cmd("INCRBY", "ki", "incrby")
cmd("DECR", "k", "decr")
cmd("DECRBY", "ki", "decrby")

-- Commands operating on lists
cmd("RPUSH", "kv", "rpush")
cmd("LPUSH", "kv", "lpush")
cmd("LLEN", "k", "llen")
cmd("LRANGE", "kse", "lrange")
cmd("LTRIM", "kse", "ltrim")
cmd("LINDEX", "ki", "lindex")
cmd("LSET", "kiv", "lset")
cmd("LREM", "kiv", "lrem")
cmd("LPOP", "k", "lpop")
cmd("RPOP", "k", "rpop")
cmd("RPOPLPUSH", "kk", "rpoplpush")

-- Commands operating on sets
cmd("SADD", "km", "sadd", true)
cmd("SREM", "km", "srem", true)
cmd("SPOP", "k", "spop")
cmd("SMOVE", "kkm", "smove")
cmd("SCARD", "k", "scard")
cmd("SISMEMBER", "km", "sismember")
cmd("SINTER", "K", "sinter")
cmd("SINTERSTORE", "kK", "sinterstore")
cmd("SUNION", "K", "sunion")
cmd("SUNIONSTORE", "kK", "sunionstore")
cmd("SDIFF", "K", "sdiff")
cmd("SDIFFSTORE", "kK", "sdiffstore")
cmd("SMEMBERS", "k", "smembers")
cmd("SRANDMEMBER", "k", "srandmember")

-- Commands operating on sorted sets
cmd("ZADD", "kfm", "zadd")
cmd("ZREM", "km", "zrem")
cmd("ZINCRBY", "kim", "zincrby")
cmd("ZRANGE", "kse", "zrange")
cmd("ZREVRANGE", "kse", "zrevrange")
cmd("ZRANGEBYSCORE", "kff", "zrangebyscore")
cmd("ZCARD", "k", "zcard")
cmd("ZSCORE", "kk", "zscore")   --FIXME
cmd("ZREMRANGEBYSCORE", "kff", "zremrangebyscore")

-- Persistence control commands
cmd("SAVE", nil, "save")
cmd("BGSAVE", nil, "bgsave")
cmd("LASTSAVE", nil, "lastsave")
cmd("SHUTDOWN", nil, "shutdown")
cmd("BGREWRITEAOF", nil, "bgrewriteaof")

-- Remote server control commands
cmd("INFO", nil, "info")
cmd("MONITOR", nil, "monitor")
cmd("PING", nil, "ping")        --not in reference...

-- Other commands

---Sort.
-- SORT key [BY pattern] [LIMIT start count] [GET pattern] [ASC|DESC] [ALPHA] [STORE dstkey]
function Connection:sort(key, t)
   local by = t.by
   local start, count = t.start, t.count
   local pattern = t.pattern
   local asc, desc, alpha = t.asc, t.desc, t.alpha
   local dstkey = t.dstkey

   assert(not(asc and desc), "ASC and DESC are mutually exclusive")
   local b = {}

   b[1] = fmt("SORT %s", tostring(key))
   if pattern then b[#b+1] = fmt("BY %s", pattern) end
   if start and count then b[#b+1] = fmt("LIMIT %d %d", start, count) end
   if asc then b[#b+1] = "ASC" elseif desc then b[#b+1] = "DESC" end
   if alpha then b[#b+1] = "ALPHA" end
   if dstkey then b[#b+1] = fmt("STORE %s", dstkey) end
   
   return self:sendrecv(send, result_handler) 
end


---Set/clear slave to another server.
function Connection:slaveof(host, port)
   if not host and not port then
      return self:sendrecv("SLAVEOF no one")
   else
      assert(host and port)
      return self:sendrecv(fmt("SLAVEOF %s %d", host, port))
   end
end
