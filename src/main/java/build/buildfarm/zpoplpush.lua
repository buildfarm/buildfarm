local zset = ARGV[1]
local dequeueName = ARGV[2]
local isblocking = false
local stringtoboolean = { ["true"]=true, ["false"]=false }
local value = ''

local function isempty(s)
  return s == nil or s == '' or type(s) == 'userdata'
end

local function isbolean(s)
  return  s == 'true' or s == 'false'
end
-- Making sure required fields are not nil
assert(not isempty(zset), 'ERR1: zset name is missing')
assert(not isempty(dequeueName), 'ERR2: Dequeue name is missing')
if not isempty(ARGV[3]) then
    assert(isbolean(ARGV[3]), 'ERR3: Has to be either true or false')
    isblocking = stringtoboolean[ARGV[3]]
end

  -- Retrieve item
  local popped = redis.call('ZRANGE', zset, 0, 0)
  -- Rotate thru popped item
  if next(popped) ~= nil then
    for _,item in ipairs(popped) do
      -- Remove leading timestamp on dequeue
      value = item:gsub("^%d*:", "")
      -- Remove item
      redis.call('ZREM', zset, item)
      -- Push to the dequeue
      redis.call('LPUSH', dequeueName, value)
    end
  end
return value