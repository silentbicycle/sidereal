require "sidereal"

--------------------------------------------------------------------
-- Test suite for Sidereal.
-- This requires LUnit ( http://nessie.de/mroth/lunit ).
--
-- The tests were adapted from Salvatore Sanfilippo's TCL test
-- suite with Emacs query-replace-regexp, keyboard macros, and
-- occasional adaptation due to the semantic mismatches between
-- TCL and Lua (counting from 1, tables, etc.).
--
-- Also, the TCL suite keeps the databases state from one test to
-- another, while the Lua tests are self-contained. (The database
-- is flushed between each test.)
-- 
-- To test non-blocking operation, set test_async to true.
--------------------------------------------------------------------

local nonblocking, trace_nb = true, false


module("tests", lunit.testcase, package.seeall)

local R
local sDEBUG = sidereal.DEBUG

local function dbg() sidereal.DEBUG = true end
local function undbg() sidereal.DEBUG = sDEBUG end

local do_slow, do_auth = false, false

local pass_ct = 0

function setup()
   local pass
   if nonblocking then
      pass = function()
                pass_ct = pass_ct + 1
                if trace_nb then print(" -- pass", pass_ct) end
             end
   end
   R = sidereal.connect("localhost", 6379, pass)
   R:flushall()                 --oh no! my data!
end


function teardown()
   R:quit()
end


--compare iter or table to expected table
local function cmp(got, exp)
   if type(got) == "function" then --iter
      for i,v in ipairs(exp) do
         assert_equal(exp[i], got())
      end
   elseif type(got) == "table" then
      for i,v in ipairs(exp) do
         assert_equal(exp[i], got[i])
      end
   end
end


local function lsort(iter)
   if type(iter) == "table" then
      table.sort(iter)
      return iter
   end
   assert(type(iter) == "function", "Bad iterator")
   local vs = {}
   for v in iter do vs[#vs+1] = v end
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


local function waitForBgsave()
   while true do
      local info = R:info(true)
      if info:match("bgsave in progress") then
         print("\nWaiting for background save to finish... ")
         socket.select(nil, nil, 1) --sleep for 1 sec
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
   R:del{"foo1", "foo2", "foo3", "foo4"}
   local res = R:mget{"foo1", "foo2", "foo3"}
   for r in res do
      assert(r == NULL)
   end
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
   if not res then fail() end
   assert_equal("key_x key_y key_z", res)
end


function test_dbsize()
   setkeys("hello", {"key_x", "key_y", "key_z", "foo_a", "foo_b", "foo_c"})
   assert_equal(6, R:dbsize())
end


function test_empty_dbsize()
   assert_equal(0, R:dbsize())
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
   R:set("novar", "    11    ")
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


function test_pipelining()
   R:send("SET k1 4\r\nxyzk\r\nGET k1\r\nPING\r\n")
   local ok, r1, r2, r3
   ok, r1 = R:receive_line()
   assert(ok)
   assert_match("OK", r1)

   ok, r2 = R:receive_line()
   assert(ok)
   assert_match("xyzk", r2)

   ok, r3 = R:receive_line()
   assert(ok)
   assert_match("PONG", r3)
end


function test_nonexist_cmd()
   local ok, err = R:sendrecv("DWIM")
   assert_false(ok)
   assert_match("unknown command", err)   --may change. test this?
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
      local ri = math.floor(math.random() * 1000)
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


function test_RPOPLPUSH_base_case_nonpipeline()
   R:rpush("mylist", "a")
   R:rpush("mylist", "b")
   R:rpush("mylist", "c")
   R:rpush("mylist", "d")

   assert_equal("d", R:rpoplpush("mylist", "newlist"))
   assert_equal("c", R:rpoplpush("mylist", "newlist"))

   local l1, err = R:lrange("mylist", 0, -1)
   assert(l1, err)
   assert_equal("a", l1())
   assert_equal("b", l1())

   local l2, err = R:lrange("newlist", 0, -1)
   assert(l2, err)
   assert_equal("c", l2())
   assert_equal("d", l2())
end


function test_RPOPLPUSH_base_case_pipeline()
   for _,v in ipairs{"a", "b", "c", "d" } do R:rpush("mylist", v) end

   local v1 = R:rpoplpush("mylist", "newlist")
   local v2 = R:rpoplpush("mylist", "newlist")
   local l1 = R:lrange("mylist", 0, -1)
   local l2 = R:lrange("newlist", 0, -1)
   assert_equal("d", v1)
   assert_equal("c", v2)
   cmp(R:lrange("mylist", 0, -1), { "a", "b" })
   cmp(R:lrange("newlist", 0, -1), { "c", "d" })
end


function test_RPOPLPUSH_with_the_same_list_as_src_and_dst()
   for _,v in ipairs{"a", "b", "c" } do R:rpush("mylist", v) end
   local l1 = R:lrange("mylist", 0, -1)
   local v = R:rpoplpush("mylist", "mylist")
   local l2 = R:lrange("mylist", 0, -1)
   for _,val in ipairs{"a", "b", "c"} do assert_equal(val, l1()) end
   assert_equal("c", v)
   for _,val in ipairs{"c", "a", "b"} do assert_equal(val, l2()) end
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
   local l = R:lrange("mylist", 0, -1)
   for _,v in ipairs{"a", "b", "c", "d"} do 
      assert_equal(v, l())
   end
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
   for k in R:keys("*") do R:del(k) end
   assert_equal(0, R:dbsize())
end


function test_DEL_all_keys_again1()
   R:select(10)
   for k in R:keys("*") do R:del(k) end
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
      assert_equal(tostring(i), g1())
   end

   for i=7,9 do
      assert_equal(tostring(i), g2())
   end

   assert_equal("4", R:lrange("mylist", 4, 4)())
end


function test_LRANGE_inverted_indexes()
   for i=0,10 do R:rpush("mylist", i) end
   local vs = {}
   for v in R:lrange("mylist", 6, 2) do
      vs[#vs+1] = v
   end
   assert_equal(0, #vs)
end


function test_LRANGE_out_of_range_indexes_including_the_full_list()
   for i=0,10 do R:rpush("mylist", i) end
   local iter = R:lrange("mylist", -1000, 1000)
   for i=0,9 do
      assert_equal(tostring(i), iter())
   end
end


function test_LRANGE_against_non_existing_key()
   local vs = {}
   for v in R:lrange("nosuchkey", 0, 1) do vs[#vs+1] = p end
   assert_equal(0, #vs)
end


function test_LTRIM_basics()
   R:del("mylist")
   for i=0,99 do
      R:lpush("mylist", i)
      R:ltrim("mylist", 0, 4)
   end
   local iter = R:lrange("mylist", 0, -1)
   for i=99,95,-1 do
      assert_equal(tostring(i), iter())
   end
end


function test_LTRIM_stress_testing()
   local mylist = {}
   local err = {}
   for i=0,19 do mylist[#mylist] = i end
   
   for j=0,99 do
      -- Fill the list
      R:del("mylist")
      for i=0,19 do
         R:rpush("mylist", i)
      end
      -- Trim at random
      local a = math.random(20)
      local b = math.random(20)
      R:ltrim("mylist", a, b)
      local l = {};
      for _,v in R:lrange("mylist", 0, -1) do
         l[#l+1] = v
      end
      for i=a,b do
         if mylist[i] ~= l[i] then fail() end
      end
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

   local ms = lsort(R:smembers("myset"))
   cmp(ms, {"bar", "foo"})
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
   cmp(lsort(R:smembers("myset")), {"bar", "ciao"})
end


function test_Mass_SADD_and_SINTER_with_two_sets()
   for i=0,999 do R:sadd("set1", i); R:sadd("set2", i + 995) end

   cmp(lsort(R:sinter("set1", "set2")),
       {"995", "996", "997", "998", "999",})
end


function test_SUNION_with_two_sets()
   for i=0,999 do R:sadd("set1", i); R:sadd("set2", i + 995) end

   local union = lsort(R:sunion("set1", "set2"))
   cmp(union, luniq(R:smembers("set1"), R:smembers("set2")))
end

    
function test_SINTERSTORE_with_two_sets()
   for i=0,999 do R:sadd("set1", i); R:sadd("set2", i + 995) end

   R:sinterstore("setres", "set1", "set2")
   cmp(lsort(R:smembers("setres")),
       {"995", "996", "997", "998", "999"})
end


function test_SINTERSTORE_with_two_sets_after_a_DEBUG_RELOAD()
   for i=0,999 do R:sadd("set1", i); R:sadd("set2", i + 995) end
   R:debug(); R:reload()
   R:sinterstore("setres", "set1", "set2")
   cmp(lsort(R:smembers("setres")),
       {"995", "996", "997", "998", "999"})
end


function test_SUNIONSTORE_with_two_sets()
   for i=0,999 do R:sadd("set1", i); R:sadd("set2", i + 995) end
   R:sunionstore("setres", "set1", "set2")
   local ms = lsort(R:smembers("setres"))
   cmp(ms, luniq(R:smembers("set1"), R:smembers("set2")))
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
   cmp(lsort(R:sinter("set1", "set2", "set3")),
       {"995", "999"})
end


function test_SINTERSTORE_with_three_sets()
   for i=0,999 do R:sadd("set1", i); R:sadd("set2", i + 995) end
   gsadd("set3", {999, 995, 1000, 2000})
   
   R:sinterstore("setres", "set1", "set2", "set3")
   cmp(lsort(R:smembers("setres")), {"995", "999"})
end


function test_SUNION_with_non_existing_keys()
   cmp(lsort(R:sunion("nokey1", "set1", "set2", "nokey2")),
       luniq(R:smembers("set1"), R:smembers("set2")))
end


function test_SDIFF_with_two_sets()
   for i=0,999 do R:sadd("set1", i) end
   for i=5,999 do R:sadd("set4", i) end
   cmp(lsort(R:sdiff("set1", "set4")),
       {"0", "1", "2", "3", "4"})
end


function test_SDIFF_with_three_sets()
   for i=0,999 do R:sadd("set1", i) end
   for i=5,999 do R:sadd("set4", i) end
   R:sadd("set5", 0)
   cmp(lsort(R:sdiff("set1", "set4", "set5")),
       {"1", "2", "3", "4"})
end


function test_SDIFFSTORE_with_three_sets()
   for i=0,999 do R:sadd("set1", i) end
   for i=5,999 do R:sadd("set4", i) end
   R:sadd("set5", 0)

   R:sdiffstore("sres", "set1", "set4", "set5")
   local ms = R:smembers("sres")
   cmp(lsort(ms), {"1", "2", "3", "4"})
end


function test_SPOP_basics()
   R:del("myset")
   R:sadd("myset", 1)
   R:sadd("myset", 2)
   R:sadd("myset", 3)
   cmp(lsort({R:spop("myset"),
              R:spop("myset"),
              R:spop("myset")}),
       {"1", "2", "3"})
   assert_equal(0, R:scard("myset"))
end


function test_SAVE_make_sure_there_are_all_the_types_as_values()
   waitForBgsave()
   R:lpush("mysavelist", "hello")
   R:lpush("mysavelist", "world")
   R:set("myemptykey", NULL)
   R:set("mynormalkey", "blablablba")
   R:zadd("mytestzset", "a", 10)
   R:zadd("mytestzset", "b", 20)
   R:zadd("mytestzset", "c", 30)
   assert_equal("OK", R:save())
end


local function test_SRANDMEMBER() -- FIXME
   R:del("myset")
   R:sadd("myset", "a")
   R:sadd("myset", "b")
   R:sadd("myset", "c")
--    unset -nocomplain myset   -- ???
   local ms = {}
   for i=0,99 do
      ms[tonumber(R:srandmember("myset"))] = 1
   end

   local ks = {}
   for k,v in pairs(ms) do ks[#ks+1] = v end
   table.sort(ks)
   cmp(ks, {"a", "b", "c"})
end

    
--[============[

function test_Create_a_random_list_and_a_random_set()
   local tosort = {}
   local seenrand = {}
   for i=0,9999 do
      while true do
         -- Make sure all the weights are different.
         -- (Neither Redis nor Lua uses a stable sort.)
         randpath {
            local rint = [expr int(rand()*1000000)]
            --                } {
            local rint = [expr rand()]
         end
         if {![info exists seenrand($rint)]} break
      end
      local seenrand =($rint) x
      R:lpush(tosort $i)
      R:sadd(tosort-set $i)
      R:set(weight_$i $rint)
      lappend tosort [list $i $rint]
   end
   local sorted = [lsort -index 1 -real $tosort]
   local res = {}
   for i=0,10000 do
      res[#res+1] = [lindex $sorted $i 0]
   end
   format {}
   --    } {}
end


function test_SORT_with_BY_against_the_newly_created_list()
        R:sort(tosort {BY weight_*})
    } $res
end


function test_the_same_SORT_with_BY,_but_against_the_newly_created_set()
        R:sort(tosort-set {BY weight_*})
    } $res
end


function test_SORT_with_BY_and_STORE_against_the_newly_created_list()
        R:sort(tosort {BY weight_*} store sort-res)
        R:lrange(sort-res 0, -1)
    } $res
end


function test_SORT_direct,_numeric,_against_the_newly_created_list()
        R:sort(tosort)
    } [lsort -integer $res]
end


function test_SORT_decreasing_sort()
        R:sort(tosort {DESC})
    } [lsort -decreasing -integer $res]
end


function test_SORT_speed,_sorting_10000_elements_list_using_BY,_100_times()
        local start = [clock clicks -milliseconds]
        for i=0,100 do
            local sorted = R:sort(tosort {BY weight_* LIMIT 0 10})
        end
        local elapsed = [expr [clock clicks -milliseconds]-$start]
        puts -nonewline "\n  Average time to sort: [expr double($elapsed)/100] milliseconds "
        flush stdout
        format {}
--    } {}
end


function test_SORT_speed,_sorting_10000_elements_list_directly,_100_times()
        local start = [clock clicks -milliseconds]
        for i=0,100 do
            local sorted = R:sort(tosort {LIMIT 0 10})
        end
        local elapsed = [expr [clock clicks -milliseconds]-$start]
        puts -nonewline "\n  Average time to sort: [expr double($elapsed)/100] milliseconds "
        flush stdout
        format {}
--    } {}
end


function test_SORT_speed,_pseudo-sorting_10000_elements_list,_BY_<const>,_100_times()
        local start = [clock clicks -milliseconds]
        for i=0,100 do
            local sorted = R:sort(tosort {BY nokey LIMIT 0 10})
        end
        local elapsed = [expr [clock clicks -milliseconds]-$start]
        puts -nonewline "\n  Average time to sort: [expr double($elapsed)/100] milliseconds "
        flush stdout
        format {}
--    } {}
end


function test_SORT_regression_for_issue_#19,_sorting_floats()
        R:flushdb()
        foreach x {1.1 5.10 3.10 7.44 2.1 5.75 6.12 0.25 1.15} {
            R:lpush("mylist" $x)
        end
        R:sort("mylist")
    } [lsort -real {1.1 5.10 3.10 7.44 2.1 5.75 6.12 0.25 1.15}]
end


function test_SORT_with_GET_ns {
        R:del("mylist")
        R:lpush("mylist" 1)
        R:lpush("mylist" 2)
        R:lpush("mylist" 3)
        R:mset(weight_1 10 weight_2 5 weight_3 30)
        R:sort("mylist" BY weight_* GET #)
    }()2 1 3}
end


function test_SORT_with_constant_GET()
        R:sort("mylist" GET, "foo")
--    } {{} {} {}}
end


function test_LREM,_remove_all_the_occurrences()
        R:flushdb()
        $r rpush "mylist" foo
        R:rpush("mylist", "bar")
        R:rpush("mylist", "foobar")
        R:rpush("mylist", "foobared")
        R:rpush("mylist", "zap")
        R:rpush("mylist", "bar")
        R:rpush("mylist", "test")
        R:rpush("mylist", "foo")
        local res = R:lrem("mylist" 0, "bar")
        list R:lrange("mylist" 0, -1) $res
--    } {{foo foobar foobared zap test foo} 2}
end


function test_LREM,_remove_the_first_occurrence()
        local res = R:lrem("mylist" 1, "foo")
        list R:lrange("mylist" 0, -1) $res
--    } {{foobar foobared zap test foo} 1}
end


function test_LREM,_remove_non_existing_element()
        local res = R:lrem("mylist" 1, "nosuchelement")
        list R:lrange("mylist" 0, -1) $res
--    } {{foobar foobared zap test foo} 0}
end


function test_LREM,_starting_from_tail_with_negative_count()
        R:flushdb()
        R:rpush("mylist", "foo")
        R:rpush("mylist", "bar")
        R:rpush("mylist", "foobar")
        R:rpush("mylist", "foobared")
        R:rpush("mylist", "zap")
        R:rpush("mylist", "bar")
        R:rpush("mylist", "test")
        R:rpush("mylist", "foo")
        R:rpush("mylist", "foo")
        local res = R:lrem("mylist" -1, "bar")
        list R:lrange("mylist" 0, -1) $res
--    } {{foo bar foobar foobared zap test foo foo} 1}
end


function test_LREM,_starting_from_tail_with_negative_count_(2)()
        local res = R:lrem("mylist" -2, "foo")
        list R:lrange("mylist" 0, -1) $res
--    } {{foo bar foobar foobared zap test} 2}
end


function test_LREM,_deleting_objects_that_may_be_encoded_as_integers()
        R:lpush(myotherlist 1)
        R:lpush(myotherlist 2)
        R:lpush(myotherlist 3)
        R:lrem(myotherlist 1 2)
        R:llen(myotherlist)
--    } {2}
end


function test_!MGET()
        R:flushdb()
        R:set(foo, "BAR")
        R:set(bar, "FOO")
        R:mget(foo, "bar")
--    } {BAR FOO}
end


function test_MGET_against_non_existing_key()
        R:mget(foo baazz, "bar")
--    } {BAR {} FOO}
end


function test_MGET_against_nonstring_key()
        R:sadd("myset", "ciao")
        R:sadd("myset", "bau")
        R:mget(foo baazz bar, "myset")
--    } {BAR {} FOO {}}
end


function test_!RANDOMKEY()
        R:flushdb()
        R:set(foo, "x")
        R:set(bar, "y")
        local foo =_seen 0
        local bar =_seen 0
        for i=0,100 do
            local rkey = R:randomkey()
            if R:ey(eq {foo)} {
                local foo =_seen 1
            end
            if R:ey(eq {bar)} {
                local bar =_seen 1
            end
        end
        list $foo_seen $bar_seen
--    } {1 1}
end


function test_RANDOMKEY_against_empty_DB()
        R:flushdb()
--        R:randomkey(    } {})
end


function test_RANDOMKEY_regression_1()
        R:flushdb()
        R:set(x 10)
        R:del(x)
        R:randomkey()
--    } {}
end


function test_GETSET_(set_new_value)()
        list R:getset(foo, "xyz") R:get(foo)
--    } {{} xyz}
end


function test_GETSET_(replace_old_value)()
        R:set(foo, "bar")
        list R:getset(foo, "xyz") R:get(foo)
--    } {bar xyz}
end


function test_SMOVE_basics()
        R:sadd(myset1, "a")
        R:sadd(myset1, "b")
        R:sadd(myset1, "c")
        R:sadd(myset2, "x")
        R:sadd(myset2, "y")
        R:sadd(myset2, "z")
        R:smove(myset1 myset2, "a")
        list [lsort R:smembers(myset2)] [lsort R:smembers(myset1)]
--    } {{a x y z} {b c}}
end


function test_SMOVE_non_existing_key()
        list R:smove(myset1 myset2, "foo") [lsort R:smembers(myset2)] [lsort R:smembers(myset1)]
--    } {0 {a x y z} {b c}}
end


function test_SMOVE_non_existing_src_set()
        list R:smove(noset myset2, "foo") [lsort R:smembers(myset2)]
--    } {0 {a x y z}}
end


function test_SMOVE_non_existing_dst_set()
        list R:smove(myset2 myset3, "y") [lsort R:smembers(myset2)] [lsort R:smembers(myset3)]
--    } {1 {a x z} y}
end


function test_SMOVE_wrong_src_key_type()
        R:set(x 10)
        local ok, err = R:smove(x myset2, "foo")
        assert_false(ok)
end


function test_SMOVE_wrong_dst_key_type()
        R:set(x 10)
        local ok, err = R:smove(myset2 x, "foo")
        assert_false(ok)
end


function test_MSET_base_case()
        R:mset(x 10 y "foo bar" z "x x x x x x x\n\n\r\n")
        R:mget(x y, "z")
    } [list 10 {foo bar} "x x x x x x x\n\n\r\n"]
end


function test_MSET_wrong_number_of_args()
        local ok, err = R:mset(x 10 y "foo bar", "z")
        assert_false(ok)
        assert_match("wrong number", err)
end


function test_MSETNX_with_already_existent_key()
        list R:msetnx(x1 xxx y2 yyy x 20) R:exists(x1) R:exists(y2)
--    } {0 0 0}
end


function test_MSETNX_with_not_existing_keys()
        list R:msetnx(x1 xxx y2, "yyy") R:get(x1) R:get(y2)
--    } {1 xxx yyy}
end


function test_MSETNX_should_remove_all_the_volatile_keys_even_on_failure()
        R:mset(x 1 y 2 z 3)
        R:expire(y 10000)
        R:expire(z 10000)
        list R:msetnx(x A y B z, "C") R:mget(x y, "z")
--    } {0 {1 {} {}}}
end


function test_ZSET_basic_ZADD_and_score_update()
        R:zadd(ztmp 10, "x")
        R:zadd(ztmp 20, "y")
        R:zadd(ztmp 30, "z")
        local aux1 = R:zrange(ztmp 0, -1)
        R:zadd(ztmp 1, "y")
        local aux2 = R:zrange(ztmp 0, -1)
        list $aux1 $aux2
--    } {{x y z} {y x z}}
end


function test_ZCARD_basics()
        R:zcard(ztmp)
--    } {3}
end


function test_ZCARD_non_existing_key()
        R:zcard(ztmp-blabla)
--    } {0}
end


function test_!ZSCORE()
        local aux = {}
        local err = {}
        for i=0,1000 do
            local score = [expr rand()]
            lappend aux $score
            R:zadd(zscoretest $score $i)
        end
        for i=0,1000 do
            if {R:zscore(zscoretest $i) != [lindex $aux $i]} {
                local err = "Expected score was [lindex $aux $i] but got R:zscore(zscoretest $i) for element $i"
                break
            end
        end
        local _ $err =
--    } {}
end


function test_ZSCORE_after_a_DEBUG_RELOAD()
        local aux = {}
        local err = {}
        R:del(zscoretest)
        for i=0,1000 do
            local score = [expr rand()]
            lappend aux $score
            R:zadd(zscoretest $score $i)
        end
        $r debug reload
        for i=0,1000 do
            if {R:zscore(zscoretest $i) != [lindex $aux $i]} {
                local err = "Expected score was [lindex $aux $i] but got R:zscore(zscoretest $i) for element $i"
                break
            end
        end
        local _ $err =
--    } {}
end


function test_ZRANGE_and_ZREVRANGE_basics()
        list R:zrange(ztmp 0, -1) R:zrevrange(ztmp 0, -1) \
            R:zrange(ztmp 1, -1) R:zrevrange(ztmp 1, -1)
--    } {{y x z} {z x y} {x z} {x y}}
end


function test_ZRANGE_WITHSCORES()
        R:zrange(ztmp 0 -1, "withscores")
--    } {y 1 x 10 z 30}
end


function test_ZSETs_stress_tester_-_sorting_is_working_well?()
        local delta = 0
        for test=0,2 do
            unset -nocomplain auxarray
            array local auxarray = {}
            local auxlist = {}
            R:del(myzset)
            for i=0,1000 do
                if {$test == 0} {
                    local score = [expr rand()]
                } else {
                    local score = [expr int(rand()*10)]
                end
                local auxarray =($i) $score
                R:zadd(myzset $score $i)
                -- Random update
                if {[expr rand()] < .2} {
                    local j = [expr int(rand()*1000)]
                    if {$test == 0} {
                        local score = [expr rand()]
                    } else {
                        local score = [expr int(rand()*10)]
                    end
                    local auxarray =($j) $score
                    R:zadd(myzset $score $j)
                end
            end
            foreach {item score} [array get auxarray] {
                lappend auxlist [list $score $item]
            end
            local sorted = [lsort -command zlistAlikeSort $auxlist]
            local auxlist = {}
            foreach x $sorted {
                lappend auxlist [lindex $x 1]
            end
            local fromredis = R:zrange(myzset 0, -1)
            local delta = 0
            for i=0,[llength $fromredis] do
                if {[lindex $fromredis $i] != [lindex $auxlist $i]} {
                    incr delta
                end
            end
        end
        format $delta
--    } {0}
end


function test_ZINCRBY_-_can_create_a_new_sorted_set()
        R:del(zset)
        R:zincrby(zset 1, "foo")
        list R:zrange(zset 0, -1) R:zscore(zset, "foo")
--    } {foo 1}
end


function test_ZINCRBY_-_increment_and_decrement()
        R:zincrby(zset 2, "foo")
        R:zincrby(zset 1, "bar")
        local v1 = [R:zrange(zset 0 -1])
        R:zincrby(zset 10, "bar")
        R:zincrby(zset -5, "foo")
        R:zincrby(zset -5, "bar")
        local v2 = [R:zrange(zset 0 -1])
        list $v1 $v2 R:zscore(zset, "foo") R:zscore(zset, "bar")
--    } {{bar foo} {foo bar} -2 6}
end


function test_ZRANGEBYSCORE_basics()
        R:del(zset)
        R:zadd(zset 1, "a")
        R:zadd(zset 2, "b")
        R:zadd(zset 3, "c")
        R:zadd(zset 4, "d")
        R:zadd(zset 5, "e")
        R:zrangebyscore(zset 2 4)
--    } {b c d}
end


function test_ZRANGEBYSCORE_fuzzy_test,_100_ranges_in_1000_elements_sorted_set()
        local err = {}
        $r del zset
        for i=0,1000 do
            $r zadd zset [expr rand()] $i
        end
        for i=0,100 do
            local min = [expr rand()]
            local max = [expr rand()]
            if {$min > $max} {
                local aux = $min
                local min = $max
                local max = $aux
            end
            local low = R:zrangebyscore(zset -inf $min)
            local ok = R:zrangebyscore(zset $min $max)
            local high = R:zrangebyscore(zset $max +inf)
            foreach x $low {
                local score = R:zscore(zset $x)
                if {$score > $min} {
                    append err "Error, score for $x is $score > $min\n"
                end
            end
            foreach x $ok {
                local score = R:zscore(zset $x)
                if {$score < $min || $score > $max} {
                    append err "Error, score for $x is $score outside $min-$max range\n"
                end
            end
            foreach x $high {
                local score = R:zscore(zset $x)
                if {$score < $max} {
                    append err "Error, score for $x is $score < $max\n"
                end
            end
        end
        local _ $err =
--    } {}
end


function test_ZRANGEBYSCORE_with_LIMIT()
        R:del(zset)
        R:zadd(zset 1, "a")
        R:zadd(zset 2, "b")
        R:zadd(zset 3, "c")
        R:zadd(zset 4, "d")
        R:zadd(zset 5, "e")
        list \
            R:zrangebyscore(zset 0 10 LIMIT 0 2) \
            R:zrangebyscore(zset 0 10 LIMIT 2 3) \
            R:zrangebyscore(zset 0 10 LIMIT 2 10) \
            R:zrangebyscore(zset 0 10 LIMIT 20 10)
--    } {{a b} {c d e} {c d e} {}}
end


function test_ZREMRANGE_basics()
        R:del(zset)
        R:zadd(zset 1, "a")
        R:zadd(zset 2, "b")
        R:zadd(zset 3, "c")
        R:zadd(zset 4, "d")
        R:zadd(zset 5, "e")
        list R:zremrangebyscore(zset 2 4) R:zrange(zset 0, -1)
--    } {3 {a e}}
end


function test_ZREMRANGE_from_-inf_to_+inf()
        R:del(zset)
        R:zadd(zset 1, "a")
        R:zadd(zset 2, "b")
        R:zadd(zset 3, "c")
        R:zadd(zset 4, "d")
        R:zadd(zset 5, "e")
        list R:zremrangebyscore(zset -inf +inf) R:zrange(zset 0, -1)
--    } {5 {}}
end


function test_SORT_against_sorted_sets()
        R:del(zset)
        R:zadd(zset 1, "a")
        R:zadd(zset 5, "b")
        R:zadd(zset 2, "c")
        R:zadd(zset 10, "d")
        R:zadd(zset 3, "e")
        R:sort(zset alpha, "desc")
--    } {e d c b a}
end


function test_Sorted_sets_+inf_and_-inf_handling()
        R:del(zset)
        R:zadd(zset -100, "a")
        R:zadd(zset 200, "b")
        R:zadd(zset -300, "c")
        R:zadd(zset 1000000, "d")
        R:zadd(zset +inf, "max")
        R:zadd(zset -inf, "min")
        R:zrange(zset 0, -1)
--    } {min c a b d max}
end


function test_EXPIRE_-_don't_set_timeouts_multiple_times()
        R:set(x, "foobar")
        local v1 = R:expire(x 5)
        local v2 = R:ttl(x)
        local v3 = R:expire(x 10)
        local v4 = R:ttl(x)
        list $v1 $v2 $v3 $v4
--    } {1 5 0 5}
end


function test_EXPIRE_-_It_should_be_still_possible_to_read_'x'()
        R:get(x)
--    } {foobar}
end


function test_EXPIRE_-_After_6_seconds_the_key_should_no_longer_be_here()
        after 6000
        list R:get(x) R:exists(x)
--    } {{} 0}
end


function test_EXPIRE_-_Delete_on_write_policy()
        R:del(x)
        R:lpush(x, "foo")
        R:expire(x 1000)
        R:lpush(x, "bar")
        R:lrange(x 0, -1)
--    } {bar}
end


function test_EXPIREAT_-_Check_for_EXPIRE_alike_behavior()
        R:del(x)
        R:set(x, "foo")
        R:expireat(x [expr [clock seconds]+15])
        R:ttl(x)
--    } {1[345]}
end


function test_ZSETs_skiplist_implementation_backlink_consistency_test()
        local diff = 0
        local elements = 10000
        for j=0,$elements do
            R:zadd(myzset [expr rand()] "Element-$j")
            R:zrem(myzset "Element-[expr int(rand()*$elements)]")
        end
        local l1 = R:zrange(myzset 0, -1)
        local l2 = R:zrevrange(myzset 0, -1)
        for j=0,[llength $l1] do
            if {[lindex $l1 $j] ne [lindex $l2 end-$j]} {
                incr diff
            end
        end
        format $diff
--    } {0}

    foreach fuzztype {binary alpha compr} {
        test "FUZZ stresser with data model $fuzztype" {
            local err = 0
            for i=0,10000 do
                local fuzz = [randstring 0 512 $fuzztype]
                R:set(foo $fuzz)
                local got = R:get(foo)
                if {$got ne $fuzz} {
                    local err = [list $fuzz $got]
                    break
                end
            end
            local _ $err =
--        } {0}
    end
end


function test_!BGSAVE()
        R:flushdb()
        R:save()
        R:set(x 10)
        R:bgsave()
        waitForBgsave $r
        R:debug(reload)
        R:get(x)
--    } {10}
end


function test_Handle_an_empty_query_well()
        local fd = R:channel)
        puts -nonewline $fd "\r\n"
        flush $fd
        R:ping()
--    } {PONG}
end


function test_Negative_multi_bulk_command_does_not_create_problems()
        local fd = R:channel)
        puts -nonewline $fd "*-10\r\n"
        flush $fd
        R:ping()
--    } {PONG}
end


function test_Negative_multi_bulk_payload()
        local fd = R:channel)
        puts -nonewline $fd "SET x -10\r\n"
        flush $fd
        gets $fd
--    } {*invalid bulk*}
end


function test_Too_big_bulk_payload()
        local fd = R:channel)
        puts -nonewline $fd "SET x 2000000000\r\n"
        flush $fd
        gets $fd
--    } {*invalid bulk*count*}
end


function test_Multi_bulk_request_not_followed_by_bulk_args()
        local fd = R:channel)
        puts -nonewline $fd "*1\r\nfoo\r\n"
        flush $fd
        gets $fd
--    } {*protocol error*}
end


function test_Generic_wrong_number_of_args()
        local ok, err = R:ping(x y, "z")
        local _ $err =
--    } {*wrong*arguments*ping*}
end


function test_SELECT_an_out_of_range_DB()
        local ok, err = R:select(1000000)
        local _ $err =
--    } {*invalid*}

end

    if {![local ok, err = {package require sha1}]} {
    function test_Check_consistency_of_different_data_types_after_a_reload()
            R:flushdb()
            createComplexDataset $r 10000
            local sha1 = [datasetDigest $r]
            R:debug(reload)
            local sha1 =_after [datasetDigest $r]
            expr {$sha1 eq $sha1_after}
--        } {1}
end


    function test_Same_dataset_digest_if_saving/reloading_as_AOF?()
            R:bgrewriteaof()
            waitForBgrewriteaof $r
            R:debug(loadaof)
            local sha1 =_after [datasetDigest $r]
            expr {$sha1 eq $sha1_after}
--        } {1}
    end
end


function test_EXPIRES_after_a_reload_(snapshot_+_append_only_file)()
        R:flushdb()
        R:set(x 10)
        R:expire(x 1000)
        R:save()
        R:debug(reload)
        local ttl = R:ttl(x)
        local e1 = [expr {$ttl > 900 && $ttl <= 1000}]
        R:bgrewriteaof()
        waitForBgrewriteaof $r
        local ttl = R:ttl(x)
        local e2 = [expr {$ttl > 900 && $ttl <= 1000}]
        list $e1 $e2
--    } {1 1}
end


function test_PIPELINING_stresser_(also_a_regression_for_the_old_epoll_bug)()
        local fd2 = [socket 127.0.0.1 6379]
        fconfigure $fd2 -encoding binary -translation binary
        puts -nonewline $fd2 "SELECT 9\r\n"
        flush $fd2
        gets $fd2

        for i=0,100000 do
            local q = {}
            local val = "0000${i}0000"
            append q "SET key:$i [string length $val]\r\n$val\r\n"
            puts -nonewline $fd2 $q
            local q = {}
            append q "GET key:$i\r\n"
            puts -nonewline $fd2 $q
        end
        flush $fd2

        for i=0,100000 do
            gets $fd2 line
            gets $fd2 count
            local count = [string range $count 1 end]
            local val = [read $fd2 $count]
            read $fd2 2
        end
        close $fd2
        local _ 1 =
--    } {1}

end

    -- Leave the user with a clean DB before to exit
function test_!FLUSHDB()
        local aux = {}
        R:select(9)
        R:flushdb()
        lappend aux R:dbsize()
        R:select(10)
        R:flushdb()
        lappend aux R:dbsize()
--    } {0 0}
end


function test_Perform_a_final_SAVE_to_leave_a_clean_DB_on_disk()
        R:save()
end

--]============]




























































-- AUTH

if do_auth then
   function test_auth()
      assert_true(R:auth(foobared))
   end
end



-- Slow tests

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
         R:set(i, "i"); sum = sum + i; end
      local expected = sum
      sum = 0
      
      assert_equal("100", R:get(100))
      
      for i=lim,1,-1 do
         local res, err = R:get(i)
         sum = sum + tonumber(res)
      end
      sidereal.DEBUG = sDEBUG
      assert_equal(expected, "sum")
   end
end