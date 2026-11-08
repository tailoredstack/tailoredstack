import type { Driver, DriverCapabilities, DriverLease, QueueSize } from "./types.ts";

// The subset of an ioredis client the driver uses. The caller creates the client; close() quits it.
export interface RedisClient {
  eval(script: string, numKeys: number, ...args: string[]): Promise<unknown>;
  quit(): Promise<unknown>;
}

export interface RedisDriverOptions {
  readonly client: RedisClient;
}

export const REDIS_CAPABILITIES: DriverCapabilities = {
  shared: true,
  exactCounts: true,
  maxPushDelayMs: 43_200_000,
};

// Keys per queue: `queue:<name>:pending` (sorted by availableAt, then push sequence) and `queue:<name>:leased`
// (sorted by leaseUntil). Each message is a hash at `queue:msg:<16-digit sequence>`. The sequence is the push
// order: it breaks ties on availableAt and never changes, so release keeps the place. Each script runs atomically.
// Message hashes are reached by computed name, so this driver targets a standalone server, not Redis Cluster.

// Returns the message member when the token names a live lease (the name matches and leaseUntil > now), else nil.
const CURRENT_LEASE = `
local function current(name, token, now)
  local member = string.format('%016d', tonumber(string.match(token, '^(%d+):')))
  local key = 'queue:msg:' .. member
  if redis.call('HGET', key, 'name') ~= name or redis.call('HGET', key, 'token') ~= token
    or tonumber(redis.call('HGET', key, 'leaseUntil')) <= tonumber(now) then
    return nil
  end
  return member
end`;

const PUSH = `
local seq = redis.call('INCR', KEYS[1])
local member = string.format('%016d', seq)
redis.call('HSET', 'queue:msg:' .. member, 'name', ARGV[1], 'payload', ARGV[2], 'availableAt', ARGV[3],
  'attempts', 0, 'leaseUntil', '', 'token', '')
redis.call('ZADD', KEYS[2], ARGV[3], member)
return string.format('%d', seq)`;

// KEYS: pending, leased. ARGV: name, now, leaseUntil.
const RESERVE = `
for _, member in ipairs(redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[2])) do
  redis.call('ZADD', KEYS[1], redis.call('HGET', 'queue:msg:' .. member, 'availableAt'), member)
  redis.call('ZREM', KEYS[2], member)
end
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[2], 'LIMIT', 0, 1)
if #due == 0 then return nil end
local member = due[1]
local key = 'queue:msg:' .. member
local attempts = redis.call('HINCRBY', key, 'attempts', 1)
local token = string.format('%d', tonumber(member)) .. ':' .. attempts
redis.call('HSET', key, 'leaseUntil', ARGV[3], 'token', token)
redis.call('ZREM', KEYS[1], member)
redis.call('ZADD', KEYS[2], ARGV[3], member)
return { string.format('%d', tonumber(member)), redis.call('HGET', key, 'payload'), attempts, token }`;

// KEYS: leased. ARGV: name, token, now.
const DELETE = `
${CURRENT_LEASE}
local member = current(ARGV[1], ARGV[2], ARGV[3])
if member == nil then return 0 end
redis.call('DEL', 'queue:msg:' .. member)
redis.call('ZREM', KEYS[1], member)
return 1`;

// KEYS: pending, leased. ARGV: name, token, availableAt, now.
const RELEASE = `
${CURRENT_LEASE}
local member = current(ARGV[1], ARGV[2], ARGV[4])
if member == nil then return 0 end
redis.call('HSET', 'queue:msg:' .. member, 'availableAt', ARGV[3], 'token', '', 'leaseUntil', '')
redis.call('ZREM', KEYS[2], member)
redis.call('ZADD', KEYS[1], ARGV[3], member)
return 1`;

// KEYS: pending, leased. ARGV: now. Expired leases are ready: their availableAt is never after the reserve time.
const SIZE = `
local now = ARGV[1]
return {
  redis.call('ZCOUNT', KEYS[2], '(' .. now, '+inf'),
  redis.call('ZCOUNT', KEYS[1], '(' .. now, '+inf'),
  redis.call('ZCOUNT', KEYS[1], '-inf', now) + redis.call('ZCOUNT', KEYS[2], '-inf', now)
}`;

export function createRedisDriver({ client }: RedisDriverOptions): Driver {
  const pending = (name: string): string => `queue:${name}:pending`;
  const leased = (name: string): string => `queue:${name}:leased`;
  const run = (script: string, keys: string[], args: string[]): Promise<unknown> =>
    client.eval(script, keys.length, ...keys, ...args);

  return {
    capabilities: REDIS_CAPABILITIES,

    async push(name, payload, at) {
      return (await run(PUSH, ["queue:seq", pending(name)], [name, payload, String(at.availableAt)])) as string;
    },

    async reserve(name, now, leaseUntil): Promise<DriverLease | null> {
      const reply = await run(RESERVE, [pending(name), leased(name)], [name, String(now), String(leaseUntil)]);
      if (reply === null) {
        return null;
      }
      const [id, payload, attempts, token] = reply as [string, string, number, string];
      return { id, payload, attempts, token };
    },

    async delete(name, token, now) {
      return (await run(DELETE, [leased(name)], [name, token, String(now)])) === 1;
    },

    async release(name, token, availableAt, now) {
      const args = [name, token, String(availableAt), String(now)];
      return (await run(RELEASE, [pending(name), leased(name)], args)) === 1;
    },

    async size(name, now): Promise<QueueSize> {
      const [reserved, delayed, ready] = (await run(SIZE, [pending(name), leased(name)], [String(now)])) as [
        number,
        number,
        number,
      ];
      return { ready, delayed, reserved };
    },

    async close() {
      await client.quit();
    },
  };
}
