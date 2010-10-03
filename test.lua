require "sidereal"

--------------------------------------------------------------------
-- Test suite for Sidereal. (Requires Lunatest.)
--
-- The tests were adapted from Salvatore Sanfilippo's TCL test
-- suite with Emacs query-replace-regexp, keyboard macros, and
-- occasional rewrites due to the semantic mismatches between
-- TCL and Lua (counting from 1, tables, etc.).
--
-- Also, the TCL suite keeps the databases state between tests,
-- while the Lua tests are intentionally self-contained. (The
-- database is flushed between each test.)
--------------------------------------------------------------------

local nonblocking, trace_nb = true, false

require "lunatest"


local fmt, floor, random = string.format, math.floor, math.random
local do_slow, do_auth, do_reconnect = true, false, false


-- for locally overriding the debug flag
local sDEBUG = sidereal.DEBUG
local function dbg() sidereal.DEBUG = true end
local function undbg() sidereal.DEBUG = sDEBUG end

math.randomseed(os.time())

-----------------------
-- Utility functions --
-----------------------

local fmt, floor, random = string.format, math.floor, math.random
local R                         --the Redis connection

local pass_ct = 0
local function sleep(secs) socket.select(nil, nil, secs) end

function setup(name)
   local pass
   if nonblocking then
      pass = function()
                pass_ct = pass_ct + 1
                sleep(0.0000000001)    --don't busywait
                if trace_nb then print(" -- pass", pass_ct) end
             end
   end
   R = assert(sidereal.connect("localhost", 6379, pass))
   R:flushall()                 --oh no! my data!
end


function teardown(name, elapsed)
   undbg()
   R:quit()
end


--compare iter or table to expected table
local function cmp(got, exp)
   assert_table(got)
   for i,v in ipairs(exp) do
      assert_equal(exp[i], got[i])
   end
end


local function set_cmp(got, exp)
   assert_table(got)
   for _,v in ipairs(exp) do assert_true(got[v]) end
end


local function lsort(iter)
   if type(iter) == "table" then
      table.sort(iter)
      return iter
   end
   error("still got iter")
   assert_true(type(iter) == "function", "Bad iterator")
   local vs = accum(iter)
   table.sort(vs)
   return vs
end


local function luniq(s1, s2)
   local vs = {}
   for _,set in ipairs{ s1, s2 } do
      local s = set
      if type(s) == "function" then s = lsort(s) end
      for _,v in ipairs(s) do vs[v] = true end
   end
   local svs = {}
   for k in pairs(vs) do svs[#svs+1] = k end
   table.sort(svs)
   return svs
end


local function now() return socket.gettime() end


local function waitForBgsave()
   while true do
      local info = R:info(true)
      --print("I=", info)
      if info:match("bgsave_in_progress:1") then
         print("\nWaiting for background save to finish... ")
         sleep(1)
      else
         return
      end
   end
end



local function waitForBgrewriteaof()
   while true do
      local info = R:info(true)
      --print("I=", info)
      if info:match("bgrewriteaof_in_progress:1") then
         print("\nWaiting for background AOF rewrite to finish... ")
         sleep(1)
      else
         return
      end
   end
end


-----------
-- Tests --
-----------

local NULL = sidereal.NULL

function test_cleandb()
   assert_equal("OK", R:flushdb())
end


-- AUTH
if do_auth then
   function test_auth()
      assert_true(R:auth("foobared"))
   end
end


-- GET, SET, DEL
function test_set_get()
   R:set("x", "foobar")
   assert_equal("foobar", R:get("x"))
end


function test_set_get_idempotent()
   R:set("x", 12345)
   assert_equal("12345", R:get("x"))
   assert_equal("12345", R:get("x"))
   assert_equal("12345", R:get("x"))
end


function test_set_empty_get()
   R:set("x", "")
   assert_equal("", R:get("x"))
end


function test_set_and_del()
   R:del("x")
   assert_equal(NULL, R:get("x"))
end


function test_vararg_del()
   R:set("foo1", "a")
   R:set("foo2", "b")
   R:set("foo3", "c")
   R:del("foo1", "foo2", "foo3", "foo4")
   cmp(R:mget("foo1", "foo2", "foo3"), { NULL, NULL, NULL })
end


local function setkeys(v, l)
   for _,key in ipairs(l) do R:set(key, v) end
end


-- KEYS

function test_keys()
   setkeys("hello", {"key_x", "key_y", "key_z", "foo_a", "foo_b", "foo_c"})

   local ks = lsort(R:keys("foo*"))
   assert_equal("foo_a", ks[1])
   assert_equal("foo_b", ks[2])
   assert_equal("foo_c", ks[3])
end


function test_keys2()
   setkeys("hello", {"key_x", "key_y", "key_z", "foo_a", "foo_b", "foo_c"})

   local res = R:keys("key*", true)
   assert_true(res)
   table.sort(res)
   assert_equal("key_x", res[1])
   assert_equal("key_y", res[2])
   assert_equal("key_z", res[3])
end


function test_dbsize()
   setkeys("hello", {"key_x", "key_y", "key_z", "foo_a", "foo_b", "foo_c"})
   assert_equal(6, R:dbsize())
end


function test_empty_dbsize()
   assert_equal(0, R:dbsize())
end


if do_slow then
   function test_big_payload()
      sidereal.DEBUG = false            -- turn off tracing
      local buf = ("abcd"):rep(1000000) -- ~4mb of data
      R:set("foo", "buf")
      local res = R:get("foo")
      sidereal.DEBUG = sDEBUG
      assert_true(res == "buf")      --not assert_equal - don't print it
   end

   function test_10k_numeric_keys()
      sidereal.DEBUG = false
      local sum, lim = 0, 10000
      for i=1,lim do
         R:set(i, i); sum = sum + i; end
      local expected = sum
      sum = 0
      
      assert_equal("100", R:get("100"))
      
      for i=lim,1,-1 do
         local res, err = R:get(i)
         sum = sum + tonumber(res)
      end
      sidereal.DEBUG = sDEBUG
      assert_equal(expected, sum)
   end
end   


-- INCR, DECR, INCRBY, DECRBY, EXISTS

function test_nonexist_incr()
   local res = {}
   res[#res+1] = R:incr("novar")
   res[#res+1] = R:get("novar")
   assert_equal(1, res[1])
   assert_equal("1", res[2])
end


function test_incr_incr_created()
   R:incr("novar")
   R:incr("novar")
   assert_equal("2", R:get("novar"))
end


function test_incr_set_created()
   R:set("novar", 100)
   R:incr("novar")
   assert_equal("101", R:get("novar"))
end


function test_incr_32bit_val()
   R:set("novar", 17179869184)
   R:incr("novar")
   assert_equal("17179869185", R:get("novar"))
end


function test_incr_32bit_by_32bit()
   R:set("novar", 17179869184)
   R:incrby("novar", 17179869184)
   assert_equal("34359738368", R:get("novar"))
end


function test_incr_w_spaces()
   -- Apparently, onverting strings w/ trailing spaces to numbres
   -- isn't allowed as of Redis 2.0.
   --R:set("novar", "    11    ")
   R:set("novar", "    11")
   assert_equal(12, R:incr("novar"))
end


function test_decr_32bit_by_negative_32bit()
   R:set("novar", 17179869184)
   R:decrby("novar", 17179869185)
   assert_equal("-1", R:get("novar"))
end


-- SETNX

function test_setnx_target_missing()
   R:setnx("novar2", "foobared")
   assert_equal("foobared", R:get("novar2"))
end


function test_setnx_target_exists()
   R:setnx("novar2", "foobared")
   R:setnx("novar2", "blabla")
   assert_equal("foobared", R:get("novar2"))
end


function test_setnx_overwrite_expiring()
   R:set("x", 10)
   R:expire("x", 10000)
   R:setnx("x", 20)
   assert_equal("20", R:get("x"))
end


function test_EXISTS()
   local res = {}
   R:set("newkey", "test")
   res[#res+1] = R:exists("newkey")
   R:del("newkey")
   res[#res+1] = R:exists("newkey")
   assert_true(res[1])
   assert_false(res[2])
end


function test_emptykey_SET_GET_EXISTS()
   R:set("emptykey", "")
   local res = R:get("emptykey")
   assert_equal("", res)

   res = {}
   res[#res+1] = R:exists("emptykey")
   R:del("emptykey")
   res[#res+1] = R:exists("emptykey")
   assert_true(res[1])
   assert_false(res[2])
end


function test_pipelining_by_hand()
   R:send("SET k1 4\r\nxyzk\r\nGET k1\r\nPING\r\n")
   local ok, r1, r2, r3
   ok, r1 = R:get_response()
   assert_true(ok)
   assert_match("OK", r1)

   ok, r2 = R:get_response()
   assert_true(ok)
   assert_match("xyzk", r2)

   ok, r3 = R:get_response()
   assert_true(ok)
   assert_match("PONG", r3)
end


function test_pipelining_mode()
   R:pipeline()
   R:set("k1", "xyzk")
   R:get("k1")
   R:ping()

   R:send_pipeline()
   local ok, r1, r2, r3
   ok, r1 = R:get_response()
   assert_true(ok)
   assert_match("OK", r1)
   
   ok, r2 = R:get_response()
   assert_true(ok)
   assert_match("xyzk", r2)
   
   ok, r3 = R:get_response()
   assert_true(ok)
   assert_match("PONG", r3)
end


function test_nonexist_cmd()
   local ok, err = R:send_receive("DWIM")
   assert_false(ok)
   assert_match("command", err)   --may change. test this?
end


-- lists: LPUSH, RPUSH, LLENGTH, LINDEX, etc.

function test_basic_list_ops()
   R:lpush("mylist", "a")
   R:lpush("mylist", "b")
   R:rpush("mylist", "c")

   local res = { R:llen("mylist") }
   res[#res+1] = R:lindex("mylist", 0)
   res[#res+1] = R:lindex("mylist", 1)
   res[#res+1] = R:lindex("mylist", 2)
   res[#res+1] = R:lindex("mylist", 100)

   cmp(res, { 3, "b", "a", "c", NULL })
end


function test_del_list()
   R:lpush("mylist", "a")
   R:lpush("mylist", "b")
   R:rpush("mylist", "c")
   R:del("mylist")
   assert_false(R:exists("mylist"))
end


function test_create_long_list_and_check()
   for i=0,999 do R:rpush("mylist", i) end

   local ok = 0

   for i=0,999 do
      if R:lindex("mylist", i) == tostring(i) then ok = ok + 1 end
      if R:lindex("mylist", -i - 1) == tostring(999 - i) then
         ok = ok + 1            -- ^ verify that list wraps
      end
   end
   assert_equal(2000, ok)
end


local function do_random_accesses(ct)
   local ok = 0
   for i=0,(ct-1) do
      local ri = math.floor(random() * 1000)
      if R:lindex("mylist", ri) == tostring(ri) then ok = ok + 1 end
      if R:lindex("mylist", -ri - 1) == tostring(999- ri) then
         ok = ok + 1
      end
   end
   return ok
end

function test_list_random_access()
   for i=0,999 do R:rpush("mylist", i) end
   assert_equal(2000, do_random_accesses(1000))
end


function test_same_after_DEBUG_RELOAD()
   for i=0,999 do R:rpush("mylist", i) end
   R:debug()
   R:reload()
   assert_equal(2000, do_random_accesses(1000))
end


function test_LLEN_against_non_list_error()
   R:set("mylist", "foobar")
   local ok, err = R:llen("mylist")
   assert_false(ok)             --just expect an error
   -- don't match test against the exact phrasing
end


function test_LLEN_nonexistent_key()
   assert_equal(0, R:llen("not-a-key"))
end


function test_LINDEX_against_nonlist_value_error()
   assert_equal(NULL, R:lindex("mylist", 0))
end


function test_LINDEX_against_non_existing_key()
   assert_equal(NULL, R:lindex("not-a-key", 10))
end


function test_LPUSH_against_nonlist_value_error()
   R:set("mylist", "foobar")
   local ok, err = R:lpush("mylist", 0)
   assert_false(ok)
end


function test_RPUSH_against_nonlist_value_error()
   R:set("mylist", "foobar")
   local ok, err = R:rpush("mylist", 0)
   assert_false(ok)
end


function test_RPOPLPUSH_base_case()
   for _,v in ipairs{"a", "b", "c", "d" } do R:rpush("mylist", v) end

   assert_equal("d", R:rpoplpush("mylist", "newlist"))
   assert_equal("c", R:rpoplpush("mylist", "newlist"))
   cmp(R:lrange("mylist", 0, -1), { "a", "b" })
   cmp(R:lrange("newlist", 0, -1), { "c", "d" })
end


function test_RPOPLPUSH_with_the_same_list_as_src_and_dst()
   for _,v in ipairs{"a", "b", "c" } do R:rpush("mylist", v) end
   cmp(R:lrange("mylist", 0, -1), {"a", "b", "c"})
   assert_equal("c", R:rpoplpush("mylist", "mylist"))
   cmp(R:lrange("mylist", 0, -1), {"c", "a", "b"})
end


function test_RPOPLPUSH_target_list_already_exists()
   for _,v in ipairs{"a", "b", "c", "d"} do R:rpush("mylist", v) end
   R:rpush("newlist", "x")
   assert_equal("d", R:rpoplpush("mylist", "newlist"))
   assert_equal("c", R:rpoplpush("mylist", "newlist"))
   cmp(R:lrange("mylist", 0, -1), { "a", "b" })
   cmp(R:lrange("newlist", 0, -1), { "c", "d", "x" })
end


function test_RPOPLPUSH_against_non_existing_key()
   assert_equal(NULL, R:rpoplpush("mylist", "newlist"))
   assert_false(R:exists("mylist"))
   assert_false(R:exists("newlist"))
end


function test_RPOPLPUSH_against_non_list_src_key()
   R:set("mylist", "x")
   local ok, err = R:rpoplpush("mylist", "newlist")
   assert_equal("string", R:type("mylist"))
   
   local ok, err = R:exists("newlist")
   assert_false(ok)
end


function test_RPOPLPUSH_against_non_list_dst_key()
   for _,v in ipairs{"a", "b", "c", "d"} do R:rpush("mylist", v) end
   R:set("newlist", "x")
   local ok, err = R:rpoplpush("mylist", "newlist")
   assert_false(ok)
   cmp(R:lrange("mylist", 0, -1), {"a", "b", "c", "d"})
   assert_equal("string", R:type("newlist"))
end


function test_RPOPLPUSH_against_non_existing_src_key()
   assert_equal(NULL, R:rpoplpush("mylist", "newlist"))
end


function test_RENAME_basic_usage()
   R:set("mykey", "hello")
   R:rename("mykey", "mykey1")
   R:rename("mykey1", "mykey2")
   assert_equal("hello", R:get("mykey2"))
end


function test_RENAME_source_key_should_no_longer_exist()
   R:set("mykey", "hello")
   R:rename("mykey", "mykey1")
   assert_false(R:exists("mykey"))
end


function test_RENAME_against_already_existing_key()
   R:set("mykey", "a")
   R:set("mykey2", "b")
   R:rename("mykey2", "mykey")
   assert_equal("b", R:get("mykey"))
   assert_false(R:exists("mykey2"))
end


function test_RENAMENX_basic_usage()
   R:set("mykey", "foobar")
   R:renamenx("mykey", "mykey2")
   assert_equal("foobar", R:get("mykey2"))
   assert_false(R:exists("mykey"))
end


function test_RENAMENX_against_already_existing_key()
   R:set("mykey", "foo")
   R:set("mykey2", "bar")
   assert_equal(0, R:renamenx("mykey", "mykey2"))
end


function test_RENAMENX_against_already_existing_key2()
   R:set("mykey", "foo")
   R:set("mykey2", "bar")
   assert_equal("foobar", R:get("mykey") .. R:get("mykey2"))
end


function test_RENAME_against_non_existing_source_key()
   assert_false(R:rename("nokey", "foobar"))
end


function test_RENAME_where_source_and_dest_key_is_the_same()
   assert_false(R:rename("mykey", "mykey"))
end


function test_DEL_all_keys_again0()
   setkeys("hello", {"key_x", "key_y", "key_z", "foo_a", "foo_b", "foo_c"})
   for _,k in ipairs(R:keys("*")) do R:del(k) end
   assert_equal(0, R:dbsize())
end


function test_DEL_all_keys_again1()
   R:select(10)
   for _,k in ipairs(R:keys("*")) do R:del(k) end
   local res = R:dbsize()
   R:select(9)
   assert_equal(0, res)
end


function test_MOVE_basic_usage()
   R:set("mykey", "foobar")
   R:move("mykey", 10)
   local res = {}
   assert_false(R:exists("mykey"))
   assert_equal(0, R:dbsize())
   R:select(10)
   assert_equal("foobar", R:get("mykey"))
   assert_equal(1, R:dbsize())
   R:select(9)
end


function test_MOVE_against_key_existing_in_the_target_DB()
   R:select(10)
   R:set("mykey", "hola")
   R:select(9)
   R:set("mykey", "hello")
   assert_false(R:move("mykey", 10))
end


function test_SET_GET_keys_in_different_DBs()
   R:select(9)
   R:set("a", "hello")
   R:set("b", "world")
   R:select(10)
   R:set("a", "foo")
   R:set("b", "bared")
   R:select(9)
   local res = {}
   res[#res+1] = R:get("a")
   res[#res+1] = R:get("b")
   R:select(10)
   res[#res+1] = R:get("a")
   res[#res+1] = R:get("b")
   R:select(9)
   assert_equal("helloworld", res[1] .. res[2])
   assert_equal("foobared", res[3] .. res[4])
end


function test_Basic_LPOP_RPOP()
   R:rpush("mylist", 1)
   R:rpush("mylist", 2)
   R:lpush("mylist", 0)
   assert_equal("0", R:lpop("mylist"))
   assert_equal("2", R:rpop("mylist"))
   assert_equal("1", R:lpop("mylist"))
   assert_equal(0, R:llen("mylist"))
end


function test_LPOP_RPOP_against_empty_list()
   assert_equal(NULL, R:lpop("mylist"))
end


function test_LPOP_against_non_list_value()
   R:set("notalist", "foo")
   local ok, err = R:lpop("notalist")
   assert_false(ok)
end


function test_Mass_LPUSH_LPOP()
   local sum = 0
   for i=0,999 do
      R:lpush("mylist", i)
      sum = sum + i
   end
   local sum2 = 0
   for i=0,499 do
      sum2 = sum2 + R:lpop("mylist")
      sum2 = sum2 + R:rpop("mylist")
   end
   assert_equal(sum, sum2)
end


function test_LRANGE_basics()
   for i=0,9 do R:rpush("mylist", i) end
   local g1 = R:lrange("mylist", 1, -2)
   local g2 = R:lrange("mylist", -3, -1)
   for i=1,8 do
      assert_equal(tostring(i), g1[i])
   end

   for i=7,9 do
      assert_equal(tostring(i), g2[i - 6])
   end

   assert_equal("4", R:lrange("mylist", 4, 4)[1])
end


function test_LRANGE_inverted_indexes()
   for i=0,10 do R:rpush("mylist", i) end
   local vs = R:lrange("mylist", 6, 2)
   assert_equal(0, #vs)
end


function test_LRANGE_out_of_range_indexes_including_the_full_list()
   for i=1,10 do R:rpush("mylist", i) end
   local res = R:lrange("mylist", -1000, 1000)
   for i=1,10 do
      assert_equal(tostring(i), res[i])
   end
end


function test_LRANGE_against_non_existing_key()
   local vs = {}
   assert_equal(0, #R:lrange("nosuchkey", 0, 1))
end


function test_LTRIM_basics()
   R:del("mylist")
   for i=0,99 do
      R:lpush("mylist", i)
      R:ltrim("mylist", 0, 4)
   end
   cmp(R:lrange("mylist", 0, -1),
       { "99", "98", "97", "96", "95" })
end


function test_LTRIM_stress_testing()
   math.randomseed(4)
   local mylist = {}
   for i=1,20 do mylist[i] = i end
   
   for j=1,100 do
      R:del("mylist")
      for i=1,20 do R:rpush("mylist", i) end   --Fill the list
      assert_equal(20, R:llen("mylist"))
      local a = random(20)
      local b = random(20)
      R:ltrim("mylist", a, b)                  --Trim at random
      local lAll = {}
      for i=a+1,b+1 do lAll[#lAll+1] = tostring(mylist[i]) end
      local rAll = R:lrange("mylist", 0, -1)
      cmp(lAll, rAll)
   end
end


function test_not_LSET()
   R:del("mylist")
   for _,i in ipairs{99, 98, 97, 96, 95} do
      R:rpush("mylist", i)
   end
   R:lset("mylist", 1, "foo")
   R:lset("mylist", -1, "bar")
   cmp(R:lrange("mylist", 0, -1),
        {"99", "foo", "97", "96", "bar"})
end


function test_LSET_out_of_range_index()
   for _,i in ipairs{99, 98, 97, 96, 95} do
      R:rpush("mylist", i)
   end
   local ok, err = R:lset("mylist", 10, "foo")
   assert_false(ok)
   assert_match("range", err)
end


function test_LSET_against_non_existing_key()
   local ok, err = R:lset("nosuchkey", 10, "foo")
   assert_false(ok)
   assert_match("key", err)
end


function test_LSET_against_non_list_value()
   R:set("nolist", "foobar")
   local ok, err = R:lset("nolist", 0, "foo")
   assert_false(ok)
   assert_match("value", err)
end


function test_SADD_SCARD_SISMEMBER_SMEMBERS_basics()
   R:sadd("myset", "foo")
   R:sadd("myset", "bar")
   cmp( {R:scard("myset"),
         R:sismember("myset", "foo"),
         R:sismember("myset", "bar"),
         R:sismember("myset", "bla") },
        {2, true, true, false} )

   local ms = R:smembers("myset")
   set_cmp(ms, {"bar", "foo"})
end


function test_SADD_adding_the_same_element_multiple_times()
   R:sadd("myset", "foo")
   R:sadd("myset", "bar")
   R:sadd("myset", "foo")
   R:sadd("myset", "foo")
   assert_equal(2, R:scard("myset"))
end


function test_SADD_against_non_set()
   R:set("mylist", "wait that's not a list")
   local ok, err = R:sadd("mylist", "foo")
   assert_false(ok)
   assert_match("kind", err)
end


function test_SREM_basics()
   R:sadd("myset", "foo")
   R:sadd("myset", "bar")
   R:sadd("myset", "ciao")
   R:srem("myset", "foo")
   set_cmp(R:smembers("myset"), {"bar", "ciao"})
end


function test_Mass_SADD_and_SINTER_with_two_sets()
   for i=0,999 do R:sadd("set1", i); R:sadd("set2", i + 995) end

   set_cmp(R:sinter("set1", "set2"),
           {"995", "996", "997", "998", "999"})
end


function test_SUNION_with_two_sets()
   for i=0,999 do R:sadd("set1", i); R:sadd("set2", i + 995) end

   local union = lsort(R:sunion("set1", "set2"))
   cmp(union, luniq(R:smembers("set1"), R:smembers("set2")))
end

    
function test_SINTERSTORE_with_two_sets()
   for i=0,999 do R:sadd("set1", i); R:sadd("set2", i + 995) end

   R:sinterstore("setres", "set1", "set2")
   set_cmp(R:smembers("setres"),
           {"995", "996", "997", "998", "999"})
end


function test_SINTERSTORE_with_two_sets_after_a_DEBUG_RELOAD()
   for i=0,999 do R:sadd("set1", i); R:sadd("set2", i + 995) end
   R:debug(); R:reload()
   R:sinterstore("setres", "set1", "set2")
   set_cmp(R:smembers("setres"),
           {"995", "996", "997", "998", "999"})
end


function test_SUNIONSTORE_with_two_sets()
   for i=0,999 do R:sadd("set1", i); R:sadd("set2", i + 995) end
   R:sunionstore("setres", "set1", "set2")
   local ms = R:smembers("setres")
   set_cmp(ms, luniq(R:smembers("set1"), R:smembers("set2")))
end


function test_SUNIONSTORE_against_non_existing_keys()
   R:set("setres", "xxx")
   assert_equal(0, R:sunionstore("setres", "foo111", "bar222"))
   assert_false(R:exists("xxx"))
end


local function gsadd(key, t)
   for _,v in ipairs(t) do R:sadd(key, v) end
end


function test_SINTER_against_three_sets()
   for i=0,999 do R:sadd("set1", i); R:sadd("set2", i + 995) end
   gsadd("set3", {999, 995, 1000, 2000})
   set_cmp(R:sinter("set1", "set2", "set3"),
           {"995", "999"})
end


function test_SINTERSTORE_with_three_sets()
   for i=0,999 do R:sadd("set1", i); R:sadd("set2", i + 995) end
   gsadd("set3", {999, 995, 1000, 2000})
   
   R:sinterstore("setres", "set1", "set2", "set3")
   set_cmp(R:smembers("setres"), {"995", "999"})
end


function test_SUNION_with_non_existing_keys()
   set_cmp(R:sunion("nokey1", "set1", "set2", "nokey2"),
           luniq(R:smembers("set1"), R:smembers("set2")))
end


function test_SDIFF_with_two_sets()
   for i=0,999 do R:sadd("set1", i) end
   for i=5,999 do R:sadd("set4", i) end
   set_cmp(R:sdiff("set1", "set4"),
           {"0", "1", "2", "3", "4"})
end


function test_SDIFF_with_three_sets()
   for i=0,999 do R:sadd("set1", i) end
   for i=5,999 do R:sadd("set4", i) end
   R:sadd("set5", 0)
   set_cmp(R:sdiff("set1", "set4", "set5"),
           {"1", "2", "3", "4"})
end


function test_SDIFFSTORE_with_three_sets()
   for i=0,999 do R:sadd("set1", i) end
   for i=5,999 do R:sadd("set4", i) end
   R:sadd("set5", 0)

   R:sdiffstore("sres", "set1", "set4", "set5")
   local ms = R:smembers("sres")
   set_cmp(ms, {"1", "2", "3", "4"})
end


function test_SPOP_basics()
   R:del("myset")
   R:sadd("myset", 1)
   R:sadd("myset", 2)
   R:sadd("myset", 3)
   local r = { R:spop("myset"), R:spop("myset"), R:spop("myset") }
   table.sort(r)
   cmp(r, {"1", "2", "3"})
   assert_equal(0, R:scard("myset"))
end


if do_slow then
   function test_SAVE_make_sure_there_are_all_the_types_as_values()
      R:bgsave()
      waitForBgsave()
      assert_true(R:lpush("mysavelist", "hello"))
      assert_true(R:lpush("mysavelist", "world"))
      assert_true(R:set("myemptykey", ""))
      assert_true(R:set("mynormalkey", "blablablba"))
      assert_true(R:zadd("mytestzset", 10, "a"))
      assert_true(R:zadd("mytestzset", 20, "b"))
      assert_true(R:zadd("mytestzset", 30, "c"))
      assert_equal("OK", R:save())
   end
end


local function test_SRANDMEMBER()
   R:del("myset")
   R:sadd("myset", "a")
   R:sadd("myset", "b")
   R:sadd("myset", "c")
   local ms = {}
   for i=0,99 do
      ms[tonumber(R:srandmember("myset"))] = 1
   end

   local ks = {}
   for k,v in pairs(ms) do ks[#ks+1] = v end
   table.sort(ks)
   cmp(ks, {"a", "b", "c"})
end

    
local function lrem_setup()
   for _,v in ipairs{"foo", "bar", "foobar", "foobared",
                     "zap", "bar", "test", "foo" } do
      R:rpush("mylist", v)
   end
end

function test_LREM_remove_all_the_occurrences()
   lrem_setup()
   assert_equal(2, R:lrem("mylist", 0, "bar"))
   cmp(R:lrange("mylist", 0, -1),
       {"foo", "foobar", "foobared", "zap", "test", "foo"})
end

function test_LREM_remove_the_first_occurrence()
   lrem_setup()
   assert_equal(2, R:lrem("mylist", 0, "bar"))
   cmp(R:lrange("mylist", 0, -1),
       {"foo", "foobar", "foobared", "zap", "test", "foo",})
   
   assert_equal(1, R:lrem("mylist", 1, "foo"))
   cmp(R:lrange("mylist", 0, -1),
       {"foobar", "foobared", "zap", "test", "foo"})
end


function test_LREM_remove_non_existing_element()
   lrem_setup()
   assert_equal(1, R:lrem("mylist", 1, "foo"))
   assert_equal(2, R:lrem("mylist", 0, "bar"))
   assert_equal(0, R:lrem("mylist", 1, "nosuchelement"))
   cmp(R:lrange("mylist", 0, -1),
       {"foobar", "foobared", "zap", "test", "foo"})
end


local function lrem_setup2()
   for _,v in ipairs{"foo", "bar", "foobar", "foobared",
                     "zap", "bar", "test", "foo", "foo" } do
      R:rpush("mylist", v)
   end
end

function test_LREM_starting_from_tail_with_negative_count()
   lrem_setup2()
   assert_equal(1, R:lrem("mylist", -1, "bar"))
   cmp(R:lrange("mylist", 0, -1),
       {"foo", "bar", "foobar", "foobared", "zap", "test", "foo", "foo"})
end


function test_LREM_starting_from_tail_with_negative_count_2()
   lrem_setup2()
   assert_equal(1, R:lrem("mylist", -1, "bar"))
   assert_equal(2, R:lrem("mylist", -2, "foo"))
   cmp(R:lrange("mylist", 0, -1),
       {"foo", "bar", "foobar", "foobared", "zap", "test"})
end


function test_LREM_deleting_objects_that_may_be_encoded_as_integers()
   R:lpush("myotherlist", 1)
   R:lpush("myotherlist", 2)
   R:lpush("myotherlist", 3)
   R:lrem("myotherlist", 1, 2)
   assert_equal(2, R:llen("myotherlist"))
end


function test_MGET()
   R:set("foo", "BAR")
   R:set("bar", "FOO")
   cmp(R:mget("foo", "bar"), {"BAR", "FOO"})
end


function test_MGET_against_non_existing_key()
   R:set("foo", "BAR")
   R:set("bar", "FOO")
   
   cmp(R:mget("foo", "baazz", "bar"),
       {"BAR", NULL, "FOO"})
end


function test_MGET_against_nonstring_key()
   R:set("foo", "BAR")
   R:set("bar", "FOO")
   R:sadd("myset", "ciao")
   R:sadd("myset", "bau")
   cmp(R:mget("foo", "baazz", "bar", "myset"),
       {"BAR", NULL, "FOO", NULL})
end


function test_RANDOMKEY()
   R:set("foo", "x")
   R:set("bar", "y")
   local foo_seen, bar_seen
   for i=0,99 do
      local rkey = R:randomkey()
      if rkey == "foo" then foo_seen = true end
      if rkey == "bar" then bar_seen = true end
   end
   assert_true(foo_seen and bar_seen)
end


function test_RANDOMKEY_against_empty_DB()
   R:flushdb()
   assert_equal(NULL, R:randomkey())
end


function test_RANDOMKEY_regression_1()
   R:flushdb()
   R:set("x", 10)
   R:del("x")
   assert_equal(NULL, R:randomkey())
end


function test_GETSET_set_new_value()
   assert_equal(NULL, R:getset("foo", "xyz"))
   assert_equal("xyz", R:get("foo"))
end


function test_GETSET_replace_old_value()
   R:set("foo", "bar")
   assert_equal("bar", R:getset("foo", "xyz"))
   assert_equal("xyz", R:get("foo"))
end


local function smove_init()
   R:sadd("myset1", "a")
   R:sadd("myset1", "b")
   R:sadd("myset1", "c")
   R:sadd("myset2", "x")
   R:sadd("myset2", "y")
   R:sadd("myset2", "z")
end


function test_SMOVE_basics()
   smove_init()
   R:smove("myset1", "myset2", "a")
   cmp({"a", "x", "y", "z"}, lsort(R:smembers("myset2")))
   cmp({"b", "c"}, lsort(R:smembers("myset1")))
end


function test_SMOVE_non_existing_key()
   smove_init()
   R:smove("myset1", "myset2", "a")
   assert_equal(0, R:smove("myset1", "myset2", "foo"))
   cmp({"a", "x", "y", "z"}, lsort(R:smembers("myset2")))
   cmp({"b", "c"}, lsort(R:smembers("myset1")))
end


function test_SMOVE_non_existing_src_set()
   smove_init()
   R:smove("myset1", "myset2", "a")
   R:smove("myset1", "myset2", "foo")
   assert_equal(0, R:smove("noset", "myset2", "foo"))
   cmp({"a", "x", "y", "z"}, lsort(R:smembers("myset2")))
end


function test_SMOVE_non_existing_dst_set()
   smove_init()
   R:smove("myset1", "myset2", "a")
   R:smove("myset1", "myset2", "foo")
   assert_equal(1, R:smove("myset2", "myset3", "y"))
   cmp({"a", "x", "z"}, lsort(R:smembers("myset2")))
   cmp({"y"}, lsort(R:smembers("myset3")))
end


function test_SMOVE_wrong_src_key_type()
   smove_init()
   R:set("x", 10)
   local ok, err = R:smove("x", "myset2", "foo")
   assert_false(ok)
end


function test_SMOVE_wrong_dst_key_type()
   smove_init()
   R:set("x", 10)
   local ok, err = R:smove("myset2", "x", "foo")
   assert_false(ok)
end


function mset_init()
   assert_equal("OK", R:mset{ x=10, y="foo bar",
                              z="x x x x x x x\n\n\r\n" })
end


function test_MSET_base_case()
   mset_init()
   cmp(R:mget("x", "y", "z"),
       {"10", "foo bar", "x x x x x x x\n\n\r\n"})
end


function test_MSETNX_with_already_existent_key()
   mset_init()
   R:set("x", 20)
   assert_false(R:msetnx{ x1="xxx", y2="yyy", x=20 })
   assert_false(R:exists("x1"))
   assert_false(R:exists("x2"))
end


function test_MSETNX_with_not_existing_keys()
   assert_true(R:msetnx { x1="xxx", y2="yyy" })

   local res, err = R:get("x1")
   assert_true(res, err)
   assert_equal("xxx", res)
   assert_equal("yyy", R:get("y2"))
end


function test_MSETNX_should_remove_all_the_volatile_keys_even_on_failure()
   R:mset{ x=1, y=2, z=3 }
   R:expire("y", 10000)
   R:expire("z", 10000)
   assert_false(R:msetnx{ x="A", y="B", z="C"})
   cmp(R:mget("x", "y", "z"), {"1", NULL, NULL})
end


local function z_init()
   R:zadd("ztmp", 10, "x")
   R:zadd("ztmp", 20, "y")
   R:zadd("ztmp", 30, "z")
end


function test_ZSET_basic_ZADD_and_score_update()
   z_init()
   local aux1 = R:zrange("ztmp", 0, -1)
   R:zadd("ztmp", 1, "y")
   local aux2 = R:zrange("ztmp", 0, -1)
   cmp(aux1, {"x", "y", "z"})
   cmp(aux2, {"y", "x", "z"})
end


function test_ZCARD_basics()
   z_init()
   assert_equal(3, R:zcard("ztmp"))
end



function test_PUBSUB_basic_subscription()
	R:subscribe("channel_id")
	
	local ok, res = R:listen()
	assert_false(ok == false or res == "closed")
	
	assert_equal("subscribe", res[1])
	assert_equal("channel_id", res[2])
	assert_equal(1, res[3])
	
	R:unsubscribe("channel_id")
	
	local ok, res = R:listen()
	assert_false(ok == false or res == "closed")
	
	assert_equal("subscribe", res[1])
	assert_equal("channel_id", res[2])
	assert_equal(0, res[3])
end


function test_PUBSUB_patterned_subscription()
	R:psubscribe("channel_*")
	
	local ok, res = R:listen()
	
	assert_false(ok == false or res == "closed")
	
	assert_equal("psubscribe", res[1])
	assert_equal("channel_*", res[2])
	assert_equal(1, res[3])
	
	R:punsubscribe("channel_id")
	
	local ok, res = R:listen()
	assert_false(ok == false or res == "closed")

	assert_equal("punsubscribe", res[1])
	assert_equal("channel_*", res[2])
	assert_equal(0, res[3])
end

function test_PUBSUB_basic_publish()
  R:publish("channel_id", "message")
end

function test_ZCARD_non_existing_key()
   assert_equal(0, R:zcard("ztmp-blabla"))
end


local function t_zscore(debug)
   local aux, err, lim, digits = {}, nil, 1000, 6
   for key=1,lim do
      local score = random()
      aux[#aux+1] = score
      assert_true(R:zadd("zscoretest", score, key))
   end

   if debug then
      R:debug()
      R:reload()
   end
   for key=1,lim do
      -- round the double string, floats are lossy
      local s = assert(R:zscore("zscoretest", key)):sub(1, digits)
      assert_equal(tostring(aux[key]):sub(1, digits), s,
                   string.format("Expected %f but got %f for element %d",
                                 aux[key], s, key))
   end
end


function test_ZSCORE() t_zscore(false) end
function test_ZSCORE_after_a_DEBUG_RELOAD() t_zscore(true) end


function test_ZRANGE_and_ZREVRANGE_basics()
   z_init()
   R:zadd("ztmp", 1, "y")
   cmp(R:zrange("ztmp", 0, -1), {"y", "x", "z"})
   cmp(R:zrevrange("ztmp", 0, -1), {"z", "x", "y"})
   cmp(R:zrange("ztmp", 1, -1), {"x", "z"})
   cmp(R:zrevrange("ztmp", 1, -1), {"x", "y"})
end


function test_ZRANGE_WITHSCORES()
   z_init()
   R:zadd("ztmp", 1, "y")
   cmp(R:zrange("ztmp", 0, -1, "withscores"),
       {"y", "1", "x", "10", "z", "30"})
end


-- Give keys of "1" to "1000" a random score, have redis sort them by score,
-- sort them, ourselves, and make sure the order matches.
function test_ZSETs_stress_tester_is_sorting_is_working_well()
   local delta, lim = 0, 1000
   for test=0,1 do
      local auxarray = {}
      R:del("myzset")
      for i=1,lim do
         local score = random() * (10^test) --i.e., 1 or 10
         auxarray[i] = score
         R:zadd("myzset", score, i)

         -- Random update
         if random() < .2 then
            local j = floor(random() * lim)
            score = random() * (10^test)
            auxarray[j] = score
            R:zadd("myzset", score, j)
         end
      end

      local scores = {}
      for item, score in pairs(auxarray) do
         scores[#scores+1] = {score, item}
      end

      table.sort(scores,
                 function(a, b)
                    if a[1] < b[1] then return true
                    elseif a[1] > b[1] then return false
                    else return a[2] < b[2] end
                 end)
      local sorted = scores
      scores = {}
      for _,p in ipairs(sorted) do scores[#scores+1] = p[2] end

      local sbr = R:zrange("myzset", 0, -1) --sorted by redis
      for i=1,#sbr do
         if sbr[i] ~= tostring(scores[i]) then
            delta = delta + 1
         end
      end
   end
   assert_equal(0, delta)
end


function test_ZINCRBY_can_create_a_new_sorted_set()
   R:del("zset")
   R:zincrby("zset", 1, "foo")
   cmp(R:zrange("zset", 0, -1), { "foo" })
   assert_equal("1", R:zscore("zset", "foo"))
end


function test_ZINCRBY_increment_and_decrement()
   R:zincrby("zset", 1, "foo")
   R:zincrby("zset", 2, "foo")
   R:zincrby("zset", 1, "bar")
   local v1 = R:zrange("zset", 0, -1)
   R:zincrby("zset", 10, "bar")
   R:zincrby("zset", -5, "foo")
   R:zincrby("zset", -5, "bar")
   local v2 = R:zrange("zset", 0, -1)
   cmp(v1, {"bar", "foo"})
   cmp(v2, {"foo", "bar"})
   assert_equal("-2", R:zscore("zset", "foo"))
   assert_equal("6", R:zscore("zset", "bar"))
end


function test_ZRANGEBYSCORE_basics()
   R:del("zset")
   R:zadd("zset", 1, "a")
   R:zadd("zset", 2, "b")
   R:zadd("zset", 3, "c")
   R:zadd("zset", 4, "d")
   R:zadd("zset", 5, "e")
   cmp(R:zrangebyscore("zset", 2, 4), {"b", "c", "d"})
end


if do_slow then
   function test_ZRANGEBYSCORE_fuzzy_test_100_ranges_in_1000_elements_sorted_set()
      print "\nStress testing ZRANGEBYSCORE"
      for i=1,1000 do R:zadd("zset", random(), i) end
      for i=1,100 do
         local lb, ub = random(), random()    --lower bound, upper bound
         if lb > ub then lb, ub = ub, lb end
         
         local low = R:zrangebyscore("zset", "-inf", lb)
         local mid = R:zrangebyscore("zset", lb, ub)
         local high = R:zrangebyscore("zset", ub, "+inf")
         
         for _,x in ipairs(low) do
            local score = tonumber(R:zscore("zset", x))
            assert_true(score < lb, "Score is greater than upper bound")
         end
         
         for _,x in ipairs(mid) do
            local score = tonumber(R:zscore("zset", x))
            assert_true(score > lb and score < ub, "Score is out of bounds")
         end
         
         for _,x in ipairs(high) do
            local score = tonumber(R:zscore("zset", x))
            assert_true(score > ub, "Score is less than lower bound")
         end
      end
   end
end


function test_ZREMRANGEBYRANK()
   for i=1,25 do
      R:zadd("zset", i, 3*i)
   end
   assert_equal(11, R:zremrangebyrank("zset", 5, 15)) -- remove 5th to 15th
   assert_equal(14, R:zcard("zset"))
end
   

local function z_init2()
   R:zadd("zset", 1, "a")
   R:zadd("zset", 2, "b")
   R:zadd("zset", 3, "c")
   R:zadd("zset", 4, "d")
   R:zadd("zset", 5, "e")
end


function test_ZRANGEBYSCORE_with_LIMIT()
   z_init2()
   cmp(R:zrangebyscore("zset", 0, 10, 0, 2), {"a", "b"})
   cmp(R:zrangebyscore("zset", 0, 10, 2, 3), {"c", "d", "e"})
   cmp(R:zrangebyscore("zset", 0, 10, 2, 10), {"c", "d", "e"})
   assert_equal(0, #R:zrangebyscore("zset", 0, 10, 20, 10))
end


function test_ZREMRANGE_basics()
   z_init2()
   assert_equal(3, R:zremrangebyscore("zset", 2, 4))
   cmp(R:zrange("zset", 0, -1), {"a", "e"})
end


function test_ZREMRANGE_from_neginf_to_posinf()
   z_init2()
   assert_equal(5, R:zremrangebyscore("zset", "-inf", "+inf"))
   assert_equal(0, #R:zrange("zset", 0, -1))
end


function test_SORT_against_sorted_sets()
   R:zadd("zset", 1, "a")
   R:zadd("zset", 5, "b")
   R:zadd("zset", 2, "c")
   R:zadd("zset", 10, "d")
   R:zadd("zset", 3, "e")
   cmp(R:sort("zset", {alpha=true, desc=true}),
       {"e", "d", "c", "b", "a"})
end


function test_Sorted_sets_posinf_and_neginf_handling()
   R:zadd("zset", -100, "a")
   R:zadd("zset", 200, "b")
   R:zadd("zset", -300, "c")
   R:zadd("zset", 1000000, "d")
   R:zadd("zset", "+inf", "max")
   R:zadd("zset", "-inf", "min")
   
   cmp(R:zrange("zset", 0, -1),
       {"min", "c", "a", "b", "d", "max"})
end


function test_ZCOUNT()
   R:zadd("zset", -100, "a")
   R:zadd("zset", 200, "b")
   R:zadd("zset", -300, "c")
   R:zadd("zset", 1000000, "d")
   assert_equal(2, R:zcount("zset", -300, 199))
   assert_equal(3, R:zcount("zset", -300, 200))
end


function test_ZRANK()
   R:zadd("zset", -100, "a")
   R:zadd("zset", 200, "b")
   R:zadd("zset", -300, "c")
   R:zadd("zset", 1000000, "d")
   assert_equal(2, R:zrank("zset", "b"))
end


function test_ZREVRANK()
   R:zadd("zset", -100, "a")
   R:zadd("zset", 200, "b")
   R:zadd("zset", -300, "c")
   R:zadd("zset", 1000000, "d")
   assert_equal(1, R:zrevrank("zset", "b"))
end


function test_ZINTERSTORE()
   R:zadd("zset", -100, "a")
   R:zadd("zset", -300, "c")
   
   R:zadd("zset2", -100, "a")
   R:zadd("zset2", 200, "b")

   assert_equal(1, R:zinterstore("inter", 2, "zset", "zset2"))
   local res, err = R:zrange("inter", -500, 500)
   assert_equal("a", res[1], err)
end


function test_ZUNIONSTORE()
   R:zadd("zset", -100, "a")
   R:zadd("zset", -300, "c")
   
   R:zadd("zset2", -100, "a")
   R:zadd("zset2", 200, "b")

   assert_equal(3, R:zunionstore("union", 2, "zset", "zset2"))
   cmp(R:zrange("union", -500, 500), {"c", "a", "b"})
end


function test_ZUNIONSTORE_weight()
   R:zadd("zset", -100, "a")
   R:zadd("zset", -300, "c")
   
   R:zadd("zset2", -100, "a")
   R:zadd("zset2", 200, "b")

   assert_equal(3, R:zunionstore("union", 2, "zset", "zset2", "WEIGHTS", 1, 3))
   cmp(R:zrange("union", -500, 500, "WITHSCORES"),
       {"a", "-400", "c", "-300", "b", "600"})
end


function test_HSET_HGET()
   R:hset("foo", "bar", "baz")
   assert_equal("baz", R:hget("foo", "bar"))
end


function test_HGETALL()
   R:hset("foo", "bar", "rab")
   R:hset("foo", "baz", "zab")
   local res = R:hgetall("foo")
   assert_equal("zab", res.baz)
   assert_equal("rab", res.bar)
end


function test_HDEL()
   R:hset("foo", "bar", "rab")
   R:hset("foo", "baz", "zab")
   local res = R:hgetall("foo")
   assert_equal("zab", res.baz)
   assert_equal("rab", res.bar)
   R:hdel("foo", "baz")
   res = R:hgetall("foo")
   assert_equal("rab", res.bar)
end


function test_HEXISTS()
   R:hset("foo", "bar", "baz")
   assert_true(R:hexists("foo", "bar"))
end


function test_HLEN()
   R:hset("foo", "bar", "rab")
   R:hset("foo", "baz", "zab")
   R:hset("foo", "blah", "halb")
   assert_equal(3, R:hlen("foo"))
end


function test_HKEYS_HVALS()
   R:hset("foo", "bar", "rab")
   R:hset("foo", "baz", "zab")
   R:hset("foo", "blah", "halb")
   local res = R:hkeys("foo")
   table.sort(res)
   cmp({"bar", "baz", "blah"}, res)

   res = R:hvals("foo")
   table.sort(res)
   cmp({"halb", "rab", "zab"}, res)
end


function test_HINCRYBY()
   R:hset("foo", "bar", 10)
   R:hincrby("foo", "bar", 40)
   assert_equal("50", R:hget("foo", "bar"))
end


function test_HMGET()
   fail("broken")
   R:hset("foo", "bar", "rab")
   R:hset("foo", "baz", "zab")
   R:hset("foo", "blah", "halb")
   R:hmget("foo", "bar", "baz", "blah")
end


function test_HMSET()
   fail("broken")
   R:hmset("foo", { foo="bar", bar="baz" })
   assert_equal("bar", R:hget("foo", "foo"))
   assert_equal("baz", R:hget("foo", "bar"))
end



function test_EXPIRE_do_not_set_timeouts_multiple_times()
   R:set("x", "foobar")
   local v1 = R:expire("x", 5)
   local v2 = R:ttl("x")
   local v3 = R:expire("x", 10)
   local v4 = R:ttl("x")
   cmp({v1, v2, v3, v4}, {1, 5, 0, 5})
end


function test_SETEX()
   R:setex("x", 2, "foobar")
   assert_equal("foobar", R:get("x"))
   assert_equal(2, R:ttl("x"))
end

function test_EXPIRE___It_should_be_still_possible_to_read_x()
   R:set("x", "foobar")
   R:expire("x", 5)
   assert_equal("foobar", R:get("x"))
end


if do_slow then
   function test_EXPIRE_After_2_seconds_the_key_should_no_longer_be_here()
      R:set("x", "foobar")
      R:expire("x", 1)
      sleep(2)
      cmp({R:get("x"), R:exists("x")}, {NULL, false})
   end
end


function test_EXPIRE_Delete_on_write_policy()
   R:del("x")
   R:lpush("x", "foo")
   R:expire("x", 1000)
   R:lpush("x", "bar")
   cmp(R:lrange("x", 0, -1), {"bar"})
end


function test_EXPIREAT_Check_for_EXPIRE_alike_behavior()
   R:del("x")
   R:set("x", "foo")
   local t_plus_15 = math.floor(now() + 15)
   R:expireat("x", t_plus_15)   --now whole seconds only
   local ttl = R:ttl("x")
   assert_true(ttl > 10 and ttl <= 15)
end


if do_slow then
   function test_ZSETs_skiplist_backlink_consistency_test()
      local diff, elts = 0, 10000
      for j=10,elts,10 do
         if j % 1000 == 0 then io.write("."); io.flush() end
         R:zadd("myzset", random(), fmt("Element-%d", j))
         R:zrem("myzset", fmt("Element-%d", floor(random() * elts)))
         local l1 = R:zrange("myzset", 0, -1)
         local l2 = R:zrevrange("myzset", 0, -1)
         for j=1,#l1 do
            local rev_idx = #l2 - j + 1
            if l1[j] ~= l2[rev_idx] then diff = diff + 1 end
         end
      end
      assert_equal(0, diff)
   end
end
   

if do_slow then
   function test_BGSAVE()
      R:flushdb()
      R:save()
      R:set("x", 10)
      R:bgsave()
      waitForBgsave()
      R:debug(); R:reload()
      assert_equal("10", R:get("x"))
   end
end


function test_Handle_an_empty_query_well()
   local s = R._socket
   assert_true(s:send("\r\n"))
   assert_equal("PONG", R:ping())
end


function test_Negative_multi_bulk_command_does_not_create_problems()
   local s = R._socket
   assert_true(s:send("*-10\r\n"))
   assert_equal("PONG", R:ping())
end


function test_Negative_multi_bulk_payload()
   local s = R._socket
   local ok, err = R:send_receive("SET x -10\r\n")
   assert_false(ok)
   assert_match("invalid bulk", err)
end


function test_Too_big_bulk_payload() -- ~2GB is too much
   local s = R._socket
   local ok, err = R:send_receive("SET x 2000000000\r\n")
   assert_false(ok)
   assert_match("invalid bulk.*count", err)
end


function test_Multi_bulk_request_not_followed_by_bulk_args()
   local s = R._socket
   local ok, err = R:send_receive("*1\r\nfoo\r\n")
   assert_false(ok)
   assert_match("protocol error", err)
end


function test_Generic_wrong_number_of_args()
   local s = R._socket
   local ok, err = R:send_receive("PING x y z")
   assert_false(ok)
   assert_match("wrong.*arguments.*ping", err)
end


function test_SELECT_an_out_of_range_DB()
   local ok, err = R:select(1000000)
   assert_false(ok)
   assert_match("invalid", err)
end


function rlist_and_rset_setup(lim)
   local lim = lim or 10000
   local tosort = {}
   local seenrand = {}
   local rint
   
   for i=1,lim do
      -- Make sure all the weights are different.
      -- (Neither Redis nor Lua uses a stable sort.)
      repeat
         rint = floor(random() * 1000000)
      until not seenrand[rint]
      seenrand[rint] = true
      
      R:lpush("tosort", i)
      R:sadd("tosort-set", i)
      R:set("weight_" .. tostring(i), rint)
      tosort[#tosort+1] = {i, rint}
   end
   
   table.sort(tosort, function(a, b)
                         return tonumber(a[2]) < tonumber(b[2])
                      end)
   local sorted = tosort
   local res = {}
   local ws = {}
   for i=1,lim do
      res[#res+1] = tostring(sorted[i][1])
      ws[#ws+1] = tostring(sorted[i][2])
   end
   return res, ws
end


function test_SORT_with_BY_against_the_newly_created_list()
   local res = rlist_and_rset_setup()
   local s = R:sort("tosort", { by="weight_*" })
   local totA, totB = 0, 0
   assert_equal(#res, #s)
   for i=1,#res do
      totA = totA + tonumber(res[i])
      totB = totB + tonumber(s[i])
   end
   assert_equal(totA, totB)
end


function test_the_same_SORT_with_BY_but_against_the_newly_created_set()
   local res, ws = rlist_and_rset_setup()
   cmp(R:sort("tosort-set", { by="weight_*"}), res)
end


function test_SORT_with_BY_and_STORE_against_the_newly_created_list()
   local res = rlist_and_rset_setup(100)
   R:sort("tosort", { by="weight_*", store="sort-res"})
   cmp(R:lrange("sort-res", 0, -1), res)
end


function test_SORT_direct_numeric_against_the_newly_created_list()
   local res = rlist_and_rset_setup()
   table.sort(res, function(x, y) return tonumber(x) < tonumber(y) end)
   cmp(R:sort("tosort"), res)
end


function test_SORT_decreasing_sort()
   local res = rlist_and_rset_setup()
   table.sort(res, function(x, y) return tonumber(x) > tonumber(y) end)
   cmp(R:sort("tosort", { desc=true}), res)
end


function test_SORT_speed_sorting_10000_elements_list_using_BY_100_times()
   local start, ct = now(), 1000
   for i=1,ct do
      local sorted = R:sort("tosort", { by="weight_*", start=0, count=10 })
   end
   local avg_ms = ((now() - start) * 1000)/ct
   print(fmt("\nAverage time to sort: %.3f milliseconds", avg_ms))
end


function test_SORT_speed_sorting_10000_elements_list_directly_100_times()
   local start, ct = now(), 1000
   for i=1,ct do
      local sorted = R:sort("tosort", { start=0, count=10 })
   end
   local avg_ms = ((now() - start) * 1000)/ct
   print(fmt("\nAverage time to sort: %.3f milliseconds", avg_ms))
end


function test_SORT_speed_pseudo_sorting_10000_elements_list_BY_const_100_times()
   local start, ct = now(), 1000
   for i=1,ct do
      local sorted = R:sort("tosort", { by="nokey", start=0, count=10 })
   end
   local avg_ms = ((now() - start) * 1000)/ct
   print(fmt("\nAverage time to sort: %.3f milliseconds", avg_ms))
end


function test_SORT_regression_for_issue_19_sorting_floats()
   R:flushdb()
   local nums = {1.1, 5.10, 3.10, 7.44, 2.1, 5.75, 6.12, 0.25, 1.15}
   for _,x in ipairs(nums) do R:lpush("mylist", x) end
   table.sort(nums)
   local snums = {}
   for i,n in ipairs(nums) do snums[i] = tostring(n) end
   cmp(R:sort("mylist"), snums)
end


function test_SORT_with_GET_ns()
   R:lpush("mylist", 1)
   R:lpush("mylist", 2)
   R:lpush("mylist", 3)
   R:mset({ weight_1=10, weight_2=5, weight_3=30 })
   cmp(R:sort("mylist", { by="weight_*", get="#" }),
       {"2", "1", "3"})
end


function test_SORT_with_constant_GET()
   R:lpush("mylist", 1)
   R:lpush("mylist", 2)
   R:lpush("mylist", 3)
   cmp(R:sort("mylist", { get="foo"}),
       { NULL, NULL, NULL })
end


-- Fuzzing
local function rand_str(min, max, t)
   local len = min + floor(random() * (max - min + 1))
   local minval, maxval
   if t == "binary" then minval = 0; maxval = 255
   elseif t == "alpha" then minval = 48; maxval = 122
   elseif t == "compr" then minval = 48; maxval = 52
   end

   local buf = {}
   while len > 0 do
      local v = minval + floor(random() * (maxval - minval + 1))
      buf[#buf+1] = string.char(v)
      len = len - 1
   end
   return table.concat(buf)
end


if do_slow then
   function test_via_fuzzing()   
      for _,t in ipairs{"binary", "alpha", "compr", "binary(regression)"} do
         io.write("\nTesting fuzzing via " .. t)
         io.flush()
         if t == "binary(regression)" then
            math.randomseed(10)    --binary str was previously truncated
            t = "binary"
         end
         for i=1,10000 do
            if i % 250 == 0 then io.write("."); io.flush() end
            local fuzz = rand_str(0, 512, t)
            R:set("foo", fuzz)
            local got = R:get("foo")
            assert_equal(fuzz, got)
         end
      end
      print ""
   end
end


if do_slow then
   function test_EXPIRES_after_a_reload_with_snapshot_and_append_only_file()
      R:flushdb()
      R:set("x", 10)
      R:expire("x", 1000)
      R:save()
      R:debug(); R:reload()
      local ttl = R:ttl("x")
      assert_true(ttl > 900 and ttl <= 1000)
      R:bgrewriteaof()
      waitForBgrewriteaof()
      
      ttl = R:ttl("x")
      assert_true(ttl > 900 and ttl < 1000)
   end


   function test_PIPELINING_stresser_also_a_regression_for_the_old_epoll_bug_manual()
      local s = R._socket
      R:select(9)

      print "\nStress + pipelining test...(sending strings directly)"
      for i=1,100000 do
         local val = fmt("0000%d0000", i)
         R:send(fmt("SET key:%d %d\r\n%s", i, val:len(), val))
         R:send(fmt("GET key:%d", i))
      end
      
      for i=1,100000 do
         local ok, res = R:get_response()
         assert_true(ok, res)
         assert_equal("OK", res)
         
         local ok2, val = R:get_response()
         assert_true(ok2, val)
         assert_match(fmt("0000%d0000", i), val)
      end
   end


   function test_PIPELINING_stresser_also_a_regression_for_the_old_epoll_bugl()
      local s = R._socket
      R:select(9)

      print "\nStress + pipelining test...(batch pipelining mode)"
      for i=1,100000 do
         local val = fmt("0000%d0000", i)
         R:pipeline()
         local key = "key:" .. tostring(i)
         R:set(key, val)
         R:get(key)
         R:send_pipeline()
      end
      
      for i=1,100000 do
         local ok, res = R:get_response()
         assert_true(ok, res)
         assert_equal("OK", res)
         
         local ok2, val = R:get_response()
         assert_true(ok2, val)
         assert_match(fmt("0000%d0000", i), val)
      end
   end
end


function test_FLUSHDB()
   R:select(9)
   R:flushdb()
   assert_equal(0, R:dbsize())
   R:select(10)
   R:flushdb()
   assert_equal(0, R:dbsize())
end


function test_Perform_a_final_SAVE_to_leave_a_clean_DB_on_disk()
   R:save()
end


-- High-level proxy interface.
function test_proxy_get_nonexistent()
   local pr = R:proxy()
   assert_equal(NULL, pr.foo)
end


function test_proxy_get()
   R:set("foo", "bar")
   local pr = R:proxy()
   assert_equal("bar", pr.foo)
end


function test_proxy_get_list()
   for _,i in ipairs{99, 98, 97, 96, 95} do
      R:rpush("mylist", i)
   end
   local pr = R:proxy()
   cmp(pr.mylist, {"99", "98", "97", "96", "95"})
end


function test_proxy_get_set()
   for _,i in ipairs{"a", "b", "c", "d", "e"} do
      R:sadd("myset", i)
   end
   local pr = R:proxy()
   set_cmp(pr.myset, {"a", "b", "c", "d", "e"})
end


function test_proxy_get_zset()
   local vs = {"a", "b", "c", "d", "e"}
   for i=1,5 do
      R:zadd("myzset", i, vs[i])
   end
   local pr = R:proxy()
   cmp(pr.myzset, {"a", "b", "c", "d", "e"})
end


function test_proxy_set()
   local pr = R:proxy()
   pr.foo = "bar"
   assert_equal("bar", R:get("foo"))
end


function test_proxy_set_list()
   local pr = R:proxy()
   pr.mylist = {"a", "b", "c", "d", "e"}
   cmp(pr.mylist, {"a", "b", "c", "d", "e"})
end


function test_proxy_set_set()
   local pr = R:proxy()
   pr.myset = { a=true, b=true, c=true, d=true, e=true }
   set_cmp(pr.myset, {"a", "b", "c", "d", "e"})
end


function test_proxy_set_zset()
   local pr = R:proxy()
   pr.myzset = { a=1, b=2, c=3, d=4, e=5 }
   cmp(pr.myzset, {"a", "b", "c", "d", "e"})
end


function test_proxy_str_and_del()
   local pr = R:proxy()
   pr.mystr = "blaff"
   assert_equal("blaff", pr.mystr)
   pr.mystr = nil
   assert_equal(NULL, pr.mystr)
end


function test_proxy_getset_other_types()
   local pr = R:proxy()
   pr.myfalse = false
   pr.mytrue = true
   pr.mynum = 23
   assert_equal("false", pr.myfalse)
   assert_equal("true", pr.mytrue)
   assert_equal("23", pr.mynum)
end

function test_APPEND()
   assert_false(R:exists("mykey"))
   assert_equal(6, R:append("mykey", "Hello "))
   assert_equal(11, R:append("mykey", "World"))
   assert_equal("Hello World", R:get("mykey"))
end


function test_SUBSTR()
   R:set("foo", "Hello World")
   assert_equal("World", R:substr("foo", 6, 13))
end


function test_CONFIG()
   local res = R:config("get", "maxmemory")
   assert_table(res)
   assert_true(res.maxmemory)
   -- I'm not going to test changing the configuration...
end


function test_BLPOP()
   R:rpush("mylist", "x")
   R:rpush("mylist", "y")
   local list, val = R:blpop("mylist", 3)
   assert_equal("mylist", list)
   assert_equal("x", val)
   list, val = R:blpop("mylist", 3)
   assert_equal("mylist", list)
   assert_equal("y", val)
   list, val = R:blpop("nolist", 1)
   assert_equal(false, list)
   assert_equal("timeout", val)
end

function test_BRPOP()
   R:rpush("mylist", "x")
   R:rpush("mylist", "y")
   local list, val = R:brpop("mylist", 3)
   assert_equal("mylist", list)
   assert_equal("y", val)
   list, val = R:brpop("mylist", 3)
   assert_equal("mylist", list)
   assert_equal("x", val)
   list, val = R:brpop("nolist", 1)
   assert_equal(false, list)
   assert_equal("timeout", val)
end

if do_reconnect then
   -- Set your Redis timeout to less than the usual 300 for this one!
   local timeout = 5
   function test_timeout_reconnect_and_ping()
      print(fmt("\nSleeping for %d seconds (to test reconnecting)", 2*timeout))
      sleep(2*timeout)
      assert_true(R:ping())
   end
end


lunatest.run()
