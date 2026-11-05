import { randomUUID } from "node:crypto";
import type { TaggableStore } from "../contracts.ts";

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
    exec(): Promise<unknown>;
  };
  incrby(key: string, increment: number): Promise<number>;
  pexpire(key: string, ms: number): Promise<number>;
  del(...keys: string[]): Promise<number>;
  eval(script: string, numKeys: number, ...args: unknown[]): Promise<unknown>;
  scanStream(options: { match: string; count: number }): AsyncIterable<unknown>;
}

export interface RedisStoreOptions {
  client: RedisClient;
  /** Namespace for every key, so entries() only sees this store's keys. */
  prefix?: string;
}

// Atomic compare-and-delete: a holder whose lock expired cannot delete its successor's lock.
const FORGET_IF = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end";

// Atomic compare-and-extend: renews only while the stored value still matches (the holder still owns the lock).
const EXTEND_IF =
  "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) else return 0 end";

const escapeGlob = (s: string) => s.replace(/[*?[\]\\]/g, "\\$&");

export class RedisStore implements TaggableStore {
  readonly #client: RedisClient;
  readonly #prefix: string;

  constructor({ client, prefix = "cache:" }: RedisStoreOptions) {
    this.#client = client;
    this.#prefix = prefix;
  }

  #key(key: string): string {
    return this.#prefix + key;
  }

  #decode(raw: unknown): unknown {
    return raw === null ? undefined : JSON.parse(raw as string);
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
    const k = this.#key(key);
    const v = JSON.stringify(value);
    if (ttlMs === null) await this.#client.set(k, v);
    else await this.#client.set(k, v, "PX", ttlMs);
  }

  async putMany(values: Record<string, unknown>, ttlMs: number | null): Promise<void> {
    const entries = Object.entries(values);
    if (entries.length === 0) return;
    const pipeline = this.#client.multi();
    for (const [key, value] of entries) {
      const k = this.#key(key);
      const v = JSON.stringify(value);
      if (ttlMs === null) pipeline.set(k, v);
      else pipeline.set(k, v, "PX", ttlMs);
    }
    await pipeline.exec();
  }

  async add(key: string, value: unknown, ttlMs: number | null): Promise<boolean> {
    const k = this.#key(key);
    const v = JSON.stringify(value);
    const ok = ttlMs === null ? await this.#client.set(k, v, "NX") : await this.#client.set(k, v, "PX", ttlMs, "NX");
    return ok === "OK";
  }

  // INCRBY keeps the existing TTL natively, matching the Store contract.
  async increment(key: string, by = 1): Promise<number> {
    return this.#client.incrby(this.#key(key), by);
  }

  async decrement(key: string, by = 1): Promise<number> {
    return this.#client.incrby(this.#key(key), -by);
  }

  async touch(key: string, ttlMs: number): Promise<boolean> {
    return (await this.#client.pexpire(this.#key(key), ttlMs)) === 1;
  }

  async forget(key: string): Promise<boolean> {
    return (await this.#client.del(this.#key(key))) === 1;
  }

  async forgetIf(key: string, expected: unknown): Promise<boolean> {
    return Number(await this.#client.eval(FORGET_IF, 1, this.#key(key), JSON.stringify(expected))) === 1;
  }

  async extendIf(key: string, expected: unknown, ttlMs: number): Promise<boolean> {
    return Number(await this.#client.eval(EXTEND_IF, 1, this.#key(key), JSON.stringify(expected), ttlMs)) === 1;
  }

  // Tag ids live outside the entry namespace (no entries() leakage) and never expire.
  #tagKey(name: string): string {
    // Cache keys are JSON arrays (start with "["), so this namespace can never collide with an entry.
    return `${this.#prefix}#tag:${name}`;
  }

  async tagId(name: string): Promise<string> {
    const k = this.#tagKey(name);
    const fresh = randomUUID();
    const won = await this.#client.set(k, fresh, "NX");
    if (won === "OK") return fresh;
    return (await this.#client.get(k)) as string;
  }

  async rotateTag(name: string): Promise<void> {
    await this.#client.set(this.#tagKey(name), randomUUID());
  }

  // SCAN, not KEYS: KEYS blocks the server on large keyspaces.
  async *entries(): AsyncGenerator<{ key: string; value: unknown }> {
    const stream = this.#client.scanStream({ match: `${escapeGlob(this.#prefix)}*`, count: 500 });
    for await (const batch of stream) {
      const keys = (batch as string[]).filter((k) => !k.startsWith(`${this.#prefix}#tag:`));
      if (keys.length === 0) continue;
      const raws = await this.#client.mget(...keys);
      for (const [i, key] of keys.entries()) {
        const raw = raws[i];
        if (typeof raw !== "string") continue; // expired between SCAN and MGET
        yield { key: key.slice(this.#prefix.length), value: JSON.parse(raw) };
      }
    }
  }
}
