local zset = ARGV[1]
local dequeueName = ARGV[2]
local value = ''

local function isempty(s)
  return s == nil or s == '' or type(s) == 'userdata'
end

-- Making sure required fields are not nil
assert(not isempty(zset), 'ERR1: zset name is missing')
assert(not isempty(dequeueName), 'ERR2: Dequeue name is missing')

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