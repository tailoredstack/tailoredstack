import type { QueueCounts, QueueDriver, RawFailedJob, RawJob } from "../driver.ts";

/**
 * The subset of a Redis client that RedisQueueDriver calls. Structural, so this module never imports the optional
 * peer. A client from the usual Node Redis library (ioredis) satisfies it.
 */
export interface RedisClient {
  eval(script: string, numKeys: number, ...args: unknown[]): Promise<unknown>;
}

export interface RedisQueueDriverOptions {
  client: RedisClient;
  /** Namespace for every key, so several queues can share one Redis. Defaults to "queue:". */
  prefix?: string;
  /**
   * Milliseconds a reservation stays hidden, per connection. Defaults to 90_000 (Laravel's Redis default).
   * Must be a positive finite number.
   */
  retryAfter?: number;
}

const DEFAULT_PREFIX = "queue:";
const DEFAULT_RETRY_AFTER_MS = 90_000;

// Job ids are INCR values padded to 20 digits. Sorted-set members with equal scores order lexically, so padding makes
// ties (same availableAt) come back in insertion order.
const ID_WIDTH = 20;

// Shared Lua helpers, prepended to each script. Keys are built from ARGV[1] (the prefix), so these scripts do not
// declare every key in KEYS. This driver targets a single Redis, not a cluster.
const LIB = `
local function jobKey(prefix, id) return prefix .. 'job:' .. id end
local function failedKey(prefix, id) return prefix .. 'failed-job:' .. id end
local function readyKey(prefix, queue) return prefix .. 'ready:' .. queue end
local function requeue(prefix, id, queue, payload, now)
  redis.call('HSET', jobKey(prefix, id), 'queue', queue, 'payload', payload, 'attempts', 0,
    'availableAt', now, 'reservedUntil', 0)
  redis.call('ZADD', readyKey(prefix, queue), now, id)
end
-- The caller holds the job: same queue, same attempt count as stored, and the lease has not ended at now.
local function held(job, queue, attempts, now)
  if redis.call('HGET', job, 'queue') ~= queue then return false end
  if tonumber(redis.call('HGET', job, 'attempts')) ~= tonumber(attempts) then return false end
  return tonumber(redis.call('HGET', job, 'reservedUntil')) > tonumber(now)
end
`;

// KEYS[1] = seq, KEYS[2] = ready(queue). ARGV = prefix, queue, payload, availableAt. Returns the padded id.
const INSERT = `${LIB}
local id = string.format('%0${ID_WIDTH}d', redis.call('INCR', KEYS[1]))
redis.call('HSET', jobKey(ARGV[1], id), 'queue', ARGV[2], 'payload', ARGV[3], 'attempts', 0,
  'availableAt', ARGV[4], 'reservedUntil', 0)
redis.call('ZADD', KEYS[2], ARGV[4], id)
return id`;

// KEYS[1] = ready(queue), KEYS[2] = reserved(queue). ARGV = prefix, now, reservedUntil.
// Expired leases become ready again first, then the oldest ready job is leased. One script, so two clients never
// lease the same job.
const RESERVE = `${LIB}
for _, id in ipairs(redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[2])) do
  local job = jobKey(ARGV[1], id)
  redis.call('ZADD', KEYS[1], redis.call('HGET', job, 'availableAt'), id)
  redis.call('ZREM', KEYS[2], id)
  redis.call('HSET', job, 'reservedUntil', 0)
end
local ready = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[2], 'LIMIT', 0, 1)
if #ready == 0 then return nil end
local id = ready[1]
local job = jobKey(ARGV[1], id)
redis.call('ZREM', KEYS[1], id)
redis.call('ZADD', KEYS[2], ARGV[3], id)
local attempts = redis.call('HINCRBY', job, 'attempts', 1)
redis.call('HSET', job, 'reservedUntil', ARGV[3])
return { id, redis.call('HGET', job, 'queue'), redis.call('HGET', job, 'payload'), attempts }`;

// KEYS[1] = reserved(queue). ARGV = prefix, id, queue, attempts, now.
const REMOVE = `${LIB}
local job = jobKey(ARGV[1], ARGV[2])
if not held(job, ARGV[3], ARGV[4], ARGV[5]) then return 0 end
redis.call('ZREM', KEYS[1], ARGV[2])
redis.call('DEL', job)
return 1`;

// KEYS[1] = reserved(queue), KEYS[2] = ready(queue). ARGV = prefix, id, queue, attempts, now, availableAt.
const RELEASE = `${LIB}
local job = jobKey(ARGV[1], ARGV[2])
if not held(job, ARGV[3], ARGV[4], ARGV[5]) then return 0 end
redis.call('ZREM', KEYS[1], ARGV[2])
redis.call('ZADD', KEYS[2], ARGV[6], ARGV[2])
redis.call('HSET', job, 'availableAt', ARGV[6], 'reservedUntil', 0)
return 1`;

// KEYS[1] = reserved(queue), KEYS[2] = failed index. ARGV = prefix, id, queue, attempts, now, error.
const FAIL = `${LIB}
local job = jobKey(ARGV[1], ARGV[2])
if not held(job, ARGV[3], ARGV[4], ARGV[5]) then return 0 end
local failed = failedKey(ARGV[1], ARGV[2])
redis.call('HSET', failed, 'queue', ARGV[3], 'payload', redis.call('HGET', job, 'payload'),
  'attempts', redis.call('HGET', job, 'attempts'), 'error', ARGV[6], 'failedAt', ARGV[5])
redis.call('ZADD', KEYS[2], ARGV[5], ARGV[2])
redis.call('ZREM', KEYS[1], ARGV[2])
redis.call('DEL', job)
return 1`;

// KEYS[1] = failed index. ARGV = prefix, queue ('' for every queue).
const FAILED_LIST = `
local out = {}
for _, id in ipairs(redis.call('ZRANGE', KEYS[1], 0, -1)) do
  local failed = ARGV[1] .. 'failed-job:' .. id
  local queue = redis.call('HGET', failed, 'queue')
  if ARGV[2] == '' or queue == ARGV[2] then
    table.insert(out, { id, queue, redis.call('HGET', failed, 'payload'), redis.call('HGET', failed, 'attempts'),
      redis.call('HGET', failed, 'error'), redis.call('HGET', failed, 'failedAt') })
  end
end
return out`;

// KEYS[1] = failed index. ARGV = prefix, id, now.
const RETRY_ONE = `${LIB}
local failed = failedKey(ARGV[1], ARGV[2])
local queue = redis.call('HGET', failed, 'queue')
if not queue then return 0 end
local payload = redis.call('HGET', failed, 'payload')
redis.call('ZREM', KEYS[1], ARGV[2])
redis.call('DEL', failed)
requeue(ARGV[1], ARGV[2], queue, payload, ARGV[3])
return 1`;

// KEYS[1] = failed index. ARGV = prefix, queue ('' for every queue), now.
const RETRY_ALL = `${LIB}
local count = 0
for _, id in ipairs(redis.call('ZRANGE', KEYS[1], 0, -1)) do
  local failed = failedKey(ARGV[1], id)
  local queue = redis.call('HGET', failed, 'queue')
  if ARGV[2] == '' or queue == ARGV[2] then
    local payload = redis.call('HGET', failed, 'payload')
    redis.call('ZREM', KEYS[1], id)
    redis.call('DEL', failed)
    requeue(ARGV[1], id, queue, payload, ARGV[3])
    count = count + 1
  end
end
return count`;

// KEYS[1] = failed index. ARGV = prefix, id.
const FORGET = `
redis.call('DEL', ARGV[1] .. 'failed-job:' .. ARGV[2])
return redis.call('ZREM', KEYS[1], ARGV[2])`;

// KEYS[1] = failed index. ARGV = prefix, queue ('' for every queue).
const FLUSH = `
local count = 0
for _, id in ipairs(redis.call('ZRANGE', KEYS[1], 0, -1)) do
  local failed = ARGV[1] .. 'failed-job:' .. id
  if ARGV[2] == '' or redis.call('HGET', failed, 'queue') == ARGV[2] then
    redis.call('DEL', failed)
    redis.call('ZREM', KEYS[1], id)
    count = count + 1
  end
end
return count`;

// KEYS[1] = failed index. ARGV = prefix, cutoff. Removes failedAt < cutoff.
const PRUNE = `
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', '(' .. ARGV[2])
for _, id in ipairs(ids) do redis.call('DEL', ARGV[1] .. 'failed-job:' .. id) end
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', '(' .. ARGV[2])
return #ids`;

// KEYS[1] = ready(queue), KEYS[2] = reserved(queue). ARGV = prefix, now. Same states as the fake and sqlite drivers:
// reserved while the lease is unexpired; otherwise delayed when availableAt is in the future; otherwise pending.
const COUNTS = `${LIB}
local pending = redis.call('ZCOUNT', KEYS[1], '-inf', ARGV[2])
local delayed = redis.call('ZCOUNT', KEYS[1], '(' .. ARGV[2], '+inf')
local reserved = redis.call('ZCOUNT', KEYS[2], '(' .. ARGV[2], '+inf')
for _, id in ipairs(redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[2])) do
  if tonumber(redis.call('HGET', jobKey(ARGV[1], id), 'availableAt')) > tonumber(ARGV[2]) then
    delayed = delayed + 1
  else
    pending = pending + 1
  end
end
return { pending, delayed, reserved }`;

/**
 * Queue driver on Redis. Every job is a hash; ready and leased jobs are sorted sets per queue; failed jobs are a hash
 * plus one index set. Each operation is one Lua script, so checks and writes never interleave with another client.
 * Times come from the core.
 */
export class RedisQueueDriver implements QueueDriver {
  readonly #client: RedisClient;
  readonly #prefix: string;
  readonly retryAfter: number;

  constructor({ client, prefix = DEFAULT_PREFIX, retryAfter = DEFAULT_RETRY_AFTER_MS }: RedisQueueDriverOptions) {
    if (!Number.isFinite(retryAfter) || retryAfter <= 0) {
      throw new RangeError(`retryAfter must be a positive finite number of ms, got ${retryAfter}`);
    }
    this.#client = client;
    this.#prefix = prefix;
    this.retryAfter = retryAfter;
  }

  #ready(queue: string): string {
    return `${this.#prefix}ready:${queue}`;
  }

  #reserved(queue: string): string {
    return `${this.#prefix}reserved:${queue}`;
  }

  #failedIndex(): string {
    return `${this.#prefix}failed`;
  }

  async insert(queue: string, payload: string, availableAt: number): Promise<string> {
    const id = await this.#client.eval(
      INSERT,
      2,
      `${this.#prefix}seq`,
      this.#ready(queue),
      this.#prefix,
      queue,
      payload,
      String(availableAt),
    );
    return String(id);
  }

  async reserve(queue: string, now: number, reservedUntil: number): Promise<RawJob | null> {
    const raw = (await this.#client.eval(
      RESERVE,
      2,
      this.#ready(queue),
      this.#reserved(queue),
      this.#prefix,
      String(now),
      String(reservedUntil),
    )) as [string, string, string, number] | null;
    if (raw === null) return null;
    const [id, job, payload, attempts] = raw;
    return { id, queue: job, payload, attempts: Number(attempts) };
  }

  async remove(queue: string, id: string, attempts: number, now: number): Promise<boolean> {
    const result = await this.#client.eval(
      REMOVE,
      1,
      this.#reserved(queue),
      this.#prefix,
      id,
      queue,
      String(attempts),
      String(now),
    );
    return Number(result) === 1;
  }

  async release(queue: string, id: string, attempts: number, now: number, availableAt: number): Promise<boolean> {
    const result = await this.#client.eval(
      RELEASE,
      2,
      this.#reserved(queue),
      this.#ready(queue),
      this.#prefix,
      id,
      queue,
      String(attempts),
      String(now),
      String(availableAt),
    );
    return Number(result) === 1;
  }

  async fail(queue: string, id: string, attempts: number, now: number, error: string): Promise<boolean> {
    const result = await this.#client.eval(
      FAIL,
      2,
      this.#reserved(queue),
      this.#failedIndex(),
      this.#prefix,
      id,
      queue,
      String(attempts),
      String(now),
      error,
    );
    return Number(result) === 1;
  }

  async failedJobs(queue: string | null): Promise<RawFailedJob[]> {
    const rows = (await this.#client.eval(FAILED_LIST, 1, this.#failedIndex(), this.#prefix, queue ?? "")) as [
      string,
      string,
      string,
      string,
      string,
      string,
    ][];
    return rows.map(([id, q, payload, attempts, error, failedAt]) => ({
      id,
      queue: q,
      payload,
      attempts: Number(attempts),
      error,
      failedAt: Number(failedAt),
    }));
  }

  async retryFailed(id: string, now: number): Promise<boolean> {
    const result = await this.#client.eval(RETRY_ONE, 1, this.#failedIndex(), this.#prefix, id, String(now));
    return Number(result) === 1;
  }

  async retryAllFailed(queue: string | null, now: number): Promise<number> {
    const result = await this.#client.eval(RETRY_ALL, 1, this.#failedIndex(), this.#prefix, queue ?? "", String(now));
    return Number(result);
  }

  async forgetFailed(id: string): Promise<boolean> {
    const result = await this.#client.eval(FORGET, 1, this.#failedIndex(), this.#prefix, id);
    return Number(result) === 1;
  }

  async flushFailed(queue: string | null): Promise<number> {
    const result = await this.#client.eval(FLUSH, 1, this.#failedIndex(), this.#prefix, queue ?? "");
    return Number(result);
  }

  async pruneFailed(cutoff: number): Promise<number> {
    const result = await this.#client.eval(PRUNE, 1, this.#failedIndex(), this.#prefix, String(cutoff));
    return Number(result);
  }

  async counts(queue: string, now: number): Promise<QueueCounts> {
    const [pending, delayed, reserved] = (await this.#client.eval(
      COUNTS,
      2,
      this.#ready(queue),
      this.#reserved(queue),
      this.#prefix,
      String(now),
    )) as [number, number, number];
    return { pending: Number(pending), delayed: Number(delayed), reserved: Number(reserved) };
  }
}
