package redis

import "github.com/redis/go-redis/v9"

// 所有 Lua 脚本只操作同一 HashTag 下的 key，保证 Valkey/Redis Cluster 兼容。
// key 命名规范：taskx:<type>:{runner_name}:<suffix>
// 花括号 {runner_name} 作为 HashTag，确保同一 runner 的所有 key 落在同一 slot。

// scriptNack 从 processing 移除并推回 pending。
// KEYS[1]=processing, KEYS[2]=pending, ARGV[1]=data
var scriptNack = redis.NewScript(`
local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed > 0 then
    redis.call('LPUSH', KEYS[2], ARGV[1])
end
return removed
`)

// scriptMoveToDead 从 processing 移到死信队列。
// KEYS[1]=processing, KEYS[2]=dead, ARGV[1]=data
var scriptMoveToDead = redis.NewScript(`
local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed > 0 then
    redis.call('LPUSH', KEYS[2], ARGV[1])
end
return removed
`)

// scriptRecoverDead 从死信队列恢复 count 条到 pending。
// KEYS[1]=dead, KEYS[2]=pending, ARGV[1]=count
var scriptRecoverDead = redis.NewScript(`
local count = tonumber(ARGV[1])
local moved = 0
for i = 1, count do
    local val = redis.call('RPOP', KEYS[1])
    if val == false then
        break
    end
    redis.call('LPUSH', KEYS[2], val)
    moved = moved + 1
end
return moved
`)

// scriptDelayTransfer 从 delay pending ZSet 取出到期元素，移入 processing ZSet。
// KEYS[1]=pending, KEYS[2]=processing, ARGV[1]=maxScore, ARGV[2]=count, ARGV[3]=processingScore
var scriptDelayTransfer = redis.NewScript(`
local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
if #items == 0 then
    return {}
end
for _, v in ipairs(items) do
    redis.call('ZREM', KEYS[1], v)
    redis.call('ZADD', KEYS[2], tonumber(ARGV[3]), v)
end
return items
`)

// scriptDelayNack 从 processing ZSet 移回 pending ZSet。
// KEYS[1]=processing, KEYS[2]=pending, ARGV[1]=data, ARGV[2]=newScore
var scriptDelayNack = redis.NewScript(`
local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if removed > 0 then
    redis.call('ZADD', KEYS[2], tonumber(ARGV[2]), ARGV[1])
end
return removed
`)

// scriptDelayMoveToDead 从 processing ZSet 移到 dead ZSet。
// KEYS[1]=processing, KEYS[2]=dead, ARGV[1]=data, ARGV[2]=deadAtScore
var scriptDelayMoveToDead = redis.NewScript(`
local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if removed > 0 then
    redis.call('ZADD', KEYS[2], tonumber(ARGV[2]), ARGV[1])
end
return removed
`)

// scriptDelayRecoverDead 从 dead ZSet 恢复 count 条到 pending ZSet。
// KEYS[1]=dead, KEYS[2]=pending, ARGV[1]=count, ARGV[2]=newScore
var scriptDelayRecoverDead = redis.NewScript(`
local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', '+inf', 'LIMIT', 0, tonumber(ARGV[1]))
local moved = 0
for _, v in ipairs(items) do
    redis.call('ZREM', KEYS[1], v)
    redis.call('ZADD', KEYS[2], tonumber(ARGV[2]), v)
    moved = moved + 1
end
return moved
`)

// scriptDelayRecoverProcessing 恢复超时的 processing 到 pending（崩溃恢复）。
// KEYS[1]=processing, KEYS[2]=pending, ARGV[1]=timeoutScore(即 now-timeout 的时间戳), ARGV[2]=newScore
var scriptDelayRecoverProcessing = redis.NewScript(`
local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
local moved = 0
for _, v in ipairs(items) do
    redis.call('ZREM', KEYS[1], v)
    redis.call('ZADD', KEYS[2], tonumber(ARGV[2]), v)
    moved = moved + 1
end
return moved
`)

// scriptEventRetryRequeue 重试时原子从 processing List 删旧值、向 pending List 推新值。
// KEYS[1]=processing, KEYS[2]=pending, ARGV[1]=oldData, ARGV[2]=newData
var scriptEventRetryRequeue = redis.NewScript(`
local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed > 0 then
    redis.call('LPUSH', KEYS[2], ARGV[2])
end
return removed
`)

// scriptDelayRetryRequeue 重试时原子从 processing ZSet 删旧值、向 pending ZSet 加新值。
// KEYS[1]=processing, KEYS[2]=pending, ARGV[1]=oldData, ARGV[2]=newData, ARGV[3]=newScore
var scriptDelayRetryRequeue = redis.NewScript(`
local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if removed > 0 then
    redis.call('ZADD', KEYS[2], tonumber(ARGV[3]), ARGV[2])
end
return removed
`)

// scriptDelayPopFromDead 原子弹出 dead ZSet 中 score 最小的元素。
// KEYS[1]=dead
var scriptDelayPopFromDead = redis.NewScript(`
local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', '+inf', 'LIMIT', 0, 1)
if #items == 0 then
    return false
end
redis.call('ZREM', KEYS[1], items[1])
return items[1]
`)

// scriptUnlock 安全释放锁（仅释放自己持有的锁）。
// KEYS[1]=lock_key, ARGV[1]=lock_value
var scriptUnlock = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
`)

// scriptRenew 续期锁（仅续期自己持有的锁）。
// KEYS[1]=lock_key, ARGV[1]=lock_value, ARGV[2]=ttl_ms
var scriptRenew = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]))
end
return 0
`)
