import type { Store } from "../contracts.ts";
import { indexPrefixesOf } from "../keys.ts";

/**
 * The subset of a Redis client that RedisStore calls. Structural, so declarations never import the optional
 * peer. A client from the usual Node Redis library satisfies it.
 */
export interface RedisClient {
  get(key: string): Promise<unknown>;
  mget(...keys: string[]): Promise<unknown[]>;
  set(key: string, value: string, ...args: unknown[]): Promise<unknown>;
  multi(): {
    set(key: string, value: string, ...args: unknown[]): unknown;
    incrby(key: string, increment: number): unknown;
    sadd(key: string, ...members: string[]): unknown;
    exec(): Promise<unknown>;
  };
  incrby(key: string, increment: number): Promise<number>;
  pexpire(key: string, ms: number): Promise<number>;
  exists(...keys: string[]): Promise<number>;
  del(...keys: string[]): Promise<number>;
  eval(script: string, numKeys: number, ...args: unknown[]): Promise<unknown>;
  scanStream(options: { match: string; count: number }): AsyncIterable<unknown>;
}

export interface RedisStoreOptions {
  client: RedisClient;
  /** Namespace for every key, so entries() only sees this store's keys. */
  prefix?: string;
}

type Transaction = ReturnType<RedisClient["multi"]>;

// Index sets live under the namespace: `${prefix}#idx:${childPrefix}`. Their members are stored keys (no namespace).
// SCAN also sees them, but MGET returns nil for a set, so entries() skips them like expired keys.
const INDEX = "#idx:";

// Atomic compare-and-delete: a holder whose lock expired cannot delete its successor's lock.
const FORGET_IF = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end";

// Atomic compare-and-extend: renews only while the stored value still matches (the holder still owns the lock).
const EXTEND_IF =
  "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) else return 0 end";

// Lists the live members of one index set and drops members whose key expired or was deleted. One script, so a
// concurrent writer's SET+SADD cannot land between the EXISTS check and the SREM. KEYS[1] = index, ARGV[1] = namespace.
const LIVE_MEMBERS = `
local live = {}
for _, member in ipairs(redis.call('SMEMBERS', KEYS[1])) do
  if redis.call('EXISTS', ARGV[1] .. member) == 1 then
    table.insert(live, member)
  else
    redis.call('SREM', KEYS[1], member)
  end
end
return live`;

// Set-if-absent that also indexes the key, in one script. KEYS[1] = value key, KEYS[2..] = index sets.
// ARGV = value, ttl ('' for none), member. Returns 1 when the value was written.
const ADD_INDEXED = `
local opts = { 'NX' }
if ARGV[2] ~= '' then
  table.insert(opts, 'PX')
  table.insert(opts, ARGV[2])
end
if not redis.call('SET', KEYS[1], ARGV[1], unpack(opts)) then return 0 end
for i = 2, #KEYS do redis.call('SADD', KEYS[i], ARGV[3]) end
return 1`;

const escapeGlob = (s: string) => s.replace(/[*?[\]\\]/g, "\\$&");

/**
 * Prefix index: every key that starts with "[" is a member of the root set and of the child set of each of its
 * leading segment runs (see indexPrefixesOf). keysWithPrefix reads one set, so it costs O(members of that set).
 *
 * Index sets have no TTL. A member whose key expired or was deleted stays until the next keysWithPrefix on that set
 * removes it. Keys without TTL that are later deleted therefore leave members behind until that query runs.
 */
export class RedisStore implements Store {
  readonly #client: RedisClient;
  readonly #prefix: string;

  constructor({ client, prefix = "cache:" }: RedisStoreOptions) {
    this.#client = client;
    this.#prefix = prefix;
  }

  #key(key: string): string {
    return this.#prefix + key;
  }

  #indexKey(childPrefix: string): string {
    return `${this.#prefix}${INDEX}${childPrefix}`;
  }

  #decode(raw: unknown): unknown {
    return raw === null ? undefined : JSON.parse(raw as string);
  }

  /** Queues the index memberships for `stored` on the same transaction as its value write. */
  #index(tx: Transaction, stored: string): void {
    for (const prefix of indexPrefixesOf(stored)) tx.sadd(this.#indexKey(prefix), stored);
  }

  #write(tx: Transaction, key: string, raw: string, ttlMs: number | null): void {
    if (ttlMs === null) tx.set(this.#key(key), raw);
    else tx.set(this.#key(key), raw, "PX", ttlMs);
    this.#index(tx, key);
  }

  async #run(tx: Transaction): Promise<[Error | null, unknown][]> {
    const results = (await tx.exec()) as [Error | null, unknown][] | null;
    if (results === null) throw new Error("Redis transaction was aborted");
    for (const [err] of results) if (err) throw err;
    return results;
  }

  async get(key: string): Promise<unknown> {
    return this.#decode(await this.#client.get(this.#key(key)));
  }

  async many(keys: string[]): Promise<unknown[]> {
    if (keys.length === 0) return [];
    const raws = await this.#client.mget(...keys.map((k) => this.#key(k)));
    return raws.map((raw) => this.#decode(raw));
  }

  async put(key: string, value: unknown, ttlMs: number | null): Promise<void> {
    const tx = this.#client.multi();
    this.#write(tx, key, JSON.stringify(value), ttlMs);
    await this.#run(tx);
  }

  async putMany(values: Record<string, unknown>, ttlMs: number | null): Promise<void> {
    const entries = Object.entries(values);
    if (entries.length === 0) return;
    const tx = this.#client.multi();
    for (const [key, value] of entries) this.#write(tx, key, JSON.stringify(value), ttlMs);
    await this.#run(tx);
  }

  async add(key: string, value: unknown, ttlMs: number | null): Promise<boolean> {
    const indexKeys = indexPrefixesOf(key).map((prefix) => this.#indexKey(prefix));
    const ok = await this.#client.eval(
      ADD_INDEXED,
      1 + indexKeys.length,
      this.#key(key),
      ...indexKeys,
      JSON.stringify(value),
      ttlMs === null ? "" : String(ttlMs),
      key,
    );
    return Number(ok) === 1;
  }

  // INCRBY keeps the existing TTL natively, matching the Store contract. Indexing on every call is idempotent.
  async increment(key: string, by = 1): Promise<number> {
    const tx = this.#client.multi();
    tx.incrby(this.#key(key), by);
    this.#index(tx, key);
    const [[, value]] = await this.#run(tx);
    return value as number;
  }

  async decrement(key: string, by = 1): Promise<number> {
    return this.increment(key, -by);
  }

  async touch(key: string, ttlMs: number): Promise<boolean> {
    return (await this.#client.pexpire(this.#key(key), ttlMs)) === 1;
  }

  // Index memberships are not removed here: keysWithPrefix prunes them lazily, so correctness never depends on it.
  async forget(key: string): Promise<boolean> {
    return (await this.#client.del(this.#key(key))) === 1;
  }

  async forgetIf(key: string, expected: unknown): Promise<boolean> {
    return Number(await this.#client.eval(FORGET_IF, 1, this.#key(key), JSON.stringify(expected))) === 1;
  }

  async extendIf(key: string, expected: unknown, ttlMs: number): Promise<boolean> {
    return Number(await this.#client.eval(EXTEND_IF, 1, this.#key(key), JSON.stringify(expected), ttlMs)) === 1;
  }

  // SCAN, not KEYS: KEYS blocks the server on large keyspaces.
  async *entries(): AsyncGenerator<{ key: string; value: unknown }> {
    const stream = this.#client.scanStream({ match: `${escapeGlob(this.#prefix)}*`, count: 500 });
    for await (const batch of stream) {
      const keys = batch as string[];
      if (keys.length === 0) continue;
      const raws = await this.#client.mget(...keys);
      for (const [i, key] of keys.entries()) {
        const raw = raws[i];
        if (typeof raw !== "string") continue; // expired between SCAN and MGET, or an index set (not a string)
        yield { key: key.slice(this.#prefix.length), value: JSON.parse(raw) };
      }
    }
  }

  /**
   * Stored keys starting with `prefix` that are still live. An exact key (ends in "]", a complete encoded array)
   * is one EXISTS: a complete array can only prefix itself. Any other prefix ("[" or a children prefix ending in ",")
   * reads its index set, which holds only matching keys.
   */
  async *keysWithPrefix(prefix: string): AsyncGenerator<string> {
    if (prefix.endsWith("]")) {
      if ((await this.#client.exists(this.#key(prefix))) === 1) yield prefix;
      return;
    }
    const members = (await this.#client.eval(LIVE_MEMBERS, 1, this.#indexKey(prefix), this.#prefix)) as string[];
    yield* members;
  }
}
