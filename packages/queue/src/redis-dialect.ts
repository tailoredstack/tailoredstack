import { randomUUID } from "node:crypto";
import type { Dialect, DialectCapabilities, Message, QueueCount, Receipt } from "./dialect.ts";
import { QueueClosedError } from "./errors.ts";

/**
 * The subset of a Redis client that RedisDialect calls. Structural, so this module never imports the optional
 * peer. An ioredis client satisfies it.
 */
export interface RedisClient {
  eval(script: string, numKeys: number, ...args: string[]): Promise<unknown>;
}

export interface RedisDialectOptions {
  client: RedisClient;
  /**
   * Namespace for every key. Defaults to `queue:`. Give each dialect its own prefix to keep several queues in one
   * Redis apart.
   */
  prefix?: string;
}

const DEFAULT_PREFIX = "queue:";

// Job ids are INCR values padded to 20 digits. Sorted-set members with equal scores order lexically, so padding makes
// ties (same availableAt) come back in insertion order.
const ID_WIDTH = 20;

// Keys are built from the prefix argument inside the scripts, so these scripts do not declare every key in KEYS.
// This dialect targets a single Redis, not a cluster.
const INSERT = `
local id = string.format('%0${ID_WIDTH}d', redis.call('INCR', KEYS[1]))
local job = ARGV[1] .. 'job:' .. id
redis.call('HSET', job, 'payload', ARGV[2], 'availableAt', ARGV[3], 'attempts', 0, 'receipt', '')
redis.call('ZADD', KEYS[2], ARGV[3], id)
return id`;

// KEYS[1] = ready(name), KEYS[2] = reserved(name). ARGV = prefix, now, leaseUntil, nonce.
// Expired leases become ready again first, then the earliest ready job is leased. One script, so two clients never
// lease the same job.
const DEQUEUE = `
local prefix = ARGV[1]
local now = ARGV[2]
for _, id in ipairs(redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now)) do
  local job = prefix .. 'job:' .. id
  local availableAt = redis.call('HGET', job, 'availableAt')
  if availableAt then
    redis.call('ZADD', KEYS[1], availableAt, id)
    redis.call('HSET', job, 'receipt', '')
  end
  redis.call('ZREM', KEYS[2], id)
end
local next = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, 1)
if #next == 0 then return nil end
local id = next[1]
local job = prefix .. 'job:' .. id
local receipt = id .. ':' .. ARGV[4]
redis.call('ZREM', KEYS[1], id)
redis.call('ZADD', KEYS[2], ARGV[3], id)
local attempts = redis.call('HINCRBY', job, 'attempts', 1)
redis.call('HSET', job, 'receipt', receipt)
return { id, redis.call('HGET', job, 'payload'), attempts, receipt }`;

// KEYS[1] = reserved(name). ARGV = prefix, id, now, receipt.
const ACKNOWLEDGE = `
local leaseUntil = redis.call('ZSCORE', KEYS[1], ARGV[2])
if not leaseUntil or tonumber(leaseUntil) <= tonumber(ARGV[3]) then return 0 end
local job = ARGV[1] .. 'job:' .. ARGV[2]
if redis.call('HGET', job, 'receipt') ~= ARGV[4] then return 0 end
redis.call('ZREM', KEYS[1], ARGV[2])
redis.call('DEL', job)
return 1`;

// KEYS[1] = reserved(name), KEYS[2] = ready(name). ARGV = prefix, id, now, receipt, availableAt.
const RELEASE = `
local leaseUntil = redis.call('ZSCORE', KEYS[1], ARGV[2])
if not leaseUntil or tonumber(leaseUntil) <= tonumber(ARGV[3]) then return 0 end
local job = ARGV[1] .. 'job:' .. ARGV[2]
if redis.call('HGET', job, 'receipt') ~= ARGV[4] then return 0 end
redis.call('ZREM', KEYS[1], ARGV[2])
redis.call('ZADD', KEYS[2], ARGV[5], ARGV[2])
redis.call('HSET', job, 'availableAt', ARGV[5], 'receipt', '')
return 1`;

// KEYS[1] = ready(name), KEYS[2] = reserved(name). ARGV = now, prefix.
// Live leases count as reserved. Expired ones are counted by their original availability, matching the other
// dialects: ready when availableAt <= now, delayed otherwise.
const COUNT = `
local now = tonumber(ARGV[1])
local ready = redis.call('ZCOUNT', KEYS[1], '-inf', ARGV[1])
local delayed = redis.call('ZCOUNT', KEYS[1], '(' .. ARGV[1], '+inf')
local reserved = redis.call('ZCOUNT', KEYS[2], '(' .. ARGV[1], '+inf')
for _, id in ipairs(redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[1])) do
  local availableAt = tonumber(redis.call('HGET', ARGV[2] .. 'job:' .. id, 'availableAt'))
  if availableAt then
    if availableAt <= now then
      ready = ready + 1
    else
      delayed = delayed + 1
    end
  end
end
return { ready, delayed, reserved }`;

/**
 * Queue dialect on Redis. Every message is a hash; ready and leased messages are sorted sets per queue name. Each
 * operation is one Lua script, so checks and writes never interleave with another client. Times come from the caller.
 *
 * The caller owns the client: `close()` only makes the dialect reject further calls, so a client shared with other
 * dialects stays open.
 */
export class RedisDialect implements Dialect {
  readonly capabilities: DialectCapabilities = {
    ordered: true,
    exactCounts: true,
    clockSource: "caller",
    maxDelayMs: Number.POSITIVE_INFINITY,
    maxLeaseMs: Number.POSITIVE_INFINITY,
  };

  readonly #client: RedisClient;
  readonly #prefix: string;
  #closed = false;

  constructor({ client, prefix = DEFAULT_PREFIX }: RedisDialectOptions) {
    this.#client = client;
    this.#prefix = prefix;
  }

  async enqueue(name: string, payload: string, at: { availableAt: number; now: number }): Promise<string> {
    this.assertOpen();
    const id = await this.#client.eval(
      INSERT,
      2,
      `${this.#prefix}seq`,
      this.#ready(name),
      this.#prefix,
      payload,
      score(at.availableAt, "availableAt"),
    );
    return String(id);
  }

  async dequeue(name: string, at: { now: number; leaseUntil: number }): Promise<Message | null> {
    this.assertOpen();
    const raw = (await this.#client.eval(
      DEQUEUE,
      2,
      this.#ready(name),
      this.#reserved(name),
      this.#prefix,
      score(at.now, "now"),
      score(at.leaseUntil, "leaseUntil"),
      randomUUID(),
    )) as [string, string, number, string] | null;
    if (raw === null) return null;
    const [id, payload, attempts, receipt] = raw;
    return { id: String(id), payload: String(payload), attempts: Number(attempts), receipt: receipt as Receipt };
  }

  async acknowledge(name: string, receipt: Receipt, now: number): Promise<boolean> {
    this.assertOpen();
    const id = idOf(receipt);
    const result = await this.#client.eval(
      ACKNOWLEDGE,
      1,
      this.#reserved(name),
      this.#prefix,
      id,
      score(now, "now"),
      receipt,
    );
    return Number(result) === 1;
  }

  async release(name: string, receipt: Receipt, at: { availableAt: number; now: number }): Promise<boolean> {
    this.assertOpen();
    const id = idOf(receipt);
    const result = await this.#client.eval(
      RELEASE,
      2,
      this.#reserved(name),
      this.#ready(name),
      this.#prefix,
      id,
      score(at.now, "now"),
      receipt,
      score(at.availableAt, "availableAt"),
    );
    return Number(result) === 1;
  }

  async count(name: string, now: number): Promise<QueueCount> {
    this.assertOpen();
    const [ready, delayed, reserved] = (await this.#client.eval(
      COUNT,
      2,
      this.#ready(name),
      this.#reserved(name),
      score(now, "now"),
      this.#prefix,
    )) as [number, number, number];
    return { ready: Number(ready), delayed: Number(delayed), reserved: Number(reserved) };
  }

  async close(): Promise<void> {
    this.#closed = true;
  }

  #ready(name: string): string {
    return `${this.#prefix}ready:${name}`;
  }

  #reserved(name: string): string {
    return `${this.#prefix}reserved:${name}`;
  }

  private assertOpen(): void {
    if (this.#closed) throw new QueueClosedError("RedisDialect is closed");
  }
}

function idOf(receipt: Receipt): string {
  return receipt.slice(0, receipt.indexOf(":"));
}

function score(value: number, label: string): string {
  if (!Number.isFinite(value)) throw new TypeError(`${label} must be a finite number`);
  return String(value);
}
