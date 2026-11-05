import { createHash } from "node:crypto";
import { isTaggable, type Store, type TaggableStore } from "./contracts.ts";
import { UnsupportedError } from "./errors.ts";
import { decodeKey, encodeKey, type Key, type KeyInput, matchKey, normalizeKey } from "./keys.ts";

export interface SetOptions {
  /** Milliseconds until expiry. A number, or a function of the value. Omit for the chained default; if none, no expiry. */
  ttl?: number | ((value: unknown) => number);
}

export interface InvalidateOptions {
  exact?: boolean;
}

export type InvalidateFilter = KeyInput | { predicate: (key: Key) => boolean; exact?: never };

export interface CacheOptions {
  /** Clock for flexible() freshness windows. Defaults to Date.now. */
  now?: () => number;
}

export interface LockOptions {
  /** How long the lock is held before it expires (ms), so a crashed holder can't block forever. */
  lockFor?: number;
  /** How long to keep retrying to acquire (ms). 0 = fail immediately. */
  waitFor?: number;
}

export interface FunnelOptions extends LockOptions {
  limit: number;
}

export type CacheEvent =
  | { type: "hit"; key: Key }
  | { type: "miss"; key: Key }
  | { type: "write"; key: Key }
  | { type: "forget"; key: Key };

export type CacheListener = (event: CacheEvent) => void;

export class LockTimeoutError extends Error {
  constructor(key: Key) {
    super(`Timed out acquiring lock for ${encodeKey(key)}`);
    this.name = "LockTimeoutError";
  }
}

// flexible() entries are stored wrapped so their freshness windows travel with the value.
const FLEX = "__cache_flexible__";
interface FlexEnvelope {
  [FLEX]: 1;
  v: unknown;
  freshUntil: number;
  staleUntil: number;
}

const isFlex = (raw: unknown): raw is FlexEnvelope =>
  typeof raw === "object" && raw !== null && (raw as Record<string, unknown>)[FLEX] === 1;

function assertTtl(ttl: number): void {
  if (!Number.isFinite(ttl) || ttl <= 0) throw new RangeError(`ttl must be a positive finite number of ms, got ${ttl}`);
}

/** Drivers take whole milliseconds (Redis PX rejects fractions). Round up so a short ttl never becomes zero. */
function toMs(ms: number): number {
  assertTtl(ms);
  return Math.ceil(ms);
}

function sleepFor(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

interface Scope {
  prefix: Key;
  ttl: number | null;
  /** Tag names when this is a tagged scope; empty for untagged caches. */
  tags: readonly string[];
}

/**
 * One API over any Store. Immutable: `prefix()` and `ttl()` return a new Cache and never mutate.
 * Listeners and clock are shared across all chained instances.
 */
export class Cache<S extends Store = Store> {
  readonly #store: S;
  readonly #scope: Scope;
  readonly #listeners: Set<CacheListener>;
  readonly #now: () => number;
  // Process-local single-flight: concurrent misses on one key share one compute.
  readonly #inflight: Map<string, Promise<unknown>>;

  constructor(
    store: S,
    options: CacheOptions = {},
    scope: Scope = { prefix: [], ttl: null, tags: [] },
    shared?: { listeners: Set<CacheListener>; now: () => number; inflight: Map<string, Promise<unknown>> },
  ) {
    this.#store = store;
    this.#scope = scope;
    this.#listeners = shared?.listeners ?? new Set();
    this.#now = shared?.now ?? options.now ?? Date.now;
    this.#inflight = shared?.inflight ?? new Map();
  }

  #derive(scope: Scope): Cache<S> {
    return new Cache<S>(this.#store, {}, scope, {
      listeners: this.#listeners,
      now: this.#now,
      inflight: this.#inflight,
    });
  }

  /** Scope every key under extra leading segments. */
  prefix(segments: KeyInput): Cache<S> {
    const extra = normalizeKey(segments);
    return this.#derive({ ...this.#scope, prefix: [...this.#scope.prefix, ...extra] });
  }

  /** Default ttl (ms) for set/getOrSet on the returned Cache. */
  ttl(ms: number): Cache<S> {
    return this.#derive({ ...this.#scope, ttl: toMs(ms) });
  }

  /**
   * Scope to a set of tags (Redis only). Writes go under a key that embeds the current tag ids; flush()
   * rotates the ids so those items become unreachable and expire on their own TTL.
   */
  tags(this: Cache<TaggableStore>, names: readonly string[]): Cache<TaggableStore> {
    if (names.length === 0) throw new RangeError("tags() needs at least one tag name");
    if (!isTaggable(this.#store)) throw new UnsupportedError("tags()", (this.#store as object).constructor.name);
    // Sorted so tag order never changes the namespace.
    return this.#derive({ ...this.#scope, tags: [...new Set(names)].sort() }) as Cache<TaggableStore>;
  }

  /** Rotate every tag in this scope. Tagged items become unreachable; untagged keys are unaffected. */
  async flush(this: Cache<TaggableStore>): Promise<void> {
    if (this.#scope.tags.length === 0)
      throw new UnsupportedError("flush()", "an untagged cache; use tags([...]).flush()");
    for (const name of this.#scope.tags) await this.#store.rotateTag(name);
  }

  /** Subscribe to hit/miss/write/forget events. Returns an unsubscribe function. */
  on(type: CacheEvent["type"], listener: CacheListener): () => void {
    const wrapped: CacheListener = (event) => {
      if (event.type === type) listener(event);
    };
    this.#listeners.add(wrapped);
    return () => {
      this.#listeners.delete(wrapped);
    };
  }

  #emit(event: CacheEvent): void {
    for (const listener of this.#listeners) listener(event);
  }

  #full(key: KeyInput): Key {
    return [...this.#scope.prefix, ...normalizeKey(key)];
  }

  #stored(key: KeyInput): string {
    return encodeKey(this.#full(key));
  }

  /** Physical key for a tagged item: sha1(tag ids) + ":" + key. Untagged caches use the plain key. */
  async #physical(key: KeyInput): Promise<string> {
    const base = this.#stored(key);
    if (this.#scope.tags.length === 0) return base;
    const store = this.#store as unknown as TaggableStore;
    const ids = await Promise.all(this.#scope.tags.map((name) => store.tagId(name)));
    return `${createHash("sha1").update(ids.join("|")).digest("hex")}:${base}`;
  }

  #ttlFor(value: unknown, options?: SetOptions): number | null {
    const ttl = options?.ttl ?? this.#scope.ttl;
    if (ttl === null || ttl === undefined) return null;
    return toMs(typeof ttl === "function" ? ttl(value) : ttl);
  }

  /** Process-local dedup: concurrent callers for one stored key share one in-flight run. */
  #singleFlight<T>(stored: string, run: () => Promise<T>): Promise<T> {
    const pending = this.#inflight.get(stored);
    if (pending) return pending as Promise<T>;
    const started = run().finally(() => this.#inflight.delete(stored));
    this.#inflight.set(stored, started);
    return started;
  }

  #assertSettable(value: unknown): void {
    if (value === undefined) throw new TypeError("Cannot cache undefined; use delete() or return null");
  }

  /** Unwraps flexible envelopes; plain values pass through. */
  #unwrap(raw: unknown): unknown {
    return isFlex(raw) ? raw.v : raw;
  }

  async get<T = unknown>(key: KeyInput): Promise<T | undefined> {
    const raw = await this.#store.get(await this.#physical(key));
    if (raw === undefined) {
      this.#emit({ type: "miss", key: this.#full(key) });
      return undefined;
    }
    this.#emit({ type: "hit", key: this.#full(key) });
    return this.#unwrap(raw) as T;
  }

  async has(key: KeyInput): Promise<boolean> {
    return (await this.#store.get(await this.#physical(key))) !== undefined;
  }

  async missing(key: KeyInput): Promise<boolean> {
    return !(await this.has(key));
  }

  async set(key: KeyInput, value: unknown, options?: SetOptions): Promise<void> {
    this.#assertSettable(value);
    await this.#store.put(await this.#physical(key), value, this.#ttlFor(value, options));
    this.#emit({ type: "write", key: this.#full(key) });
  }

  async delete(key: KeyInput): Promise<boolean> {
    const removed = await this.#store.forget(await this.#physical(key));
    if (removed) this.#emit({ type: "forget", key: this.#full(key) });
    return removed;
  }

  async deleteMultiple(keys: KeyInput[]): Promise<number> {
    let removed = 0;
    for (const key of keys) if (await this.delete(key)) removed++;
    return removed;
  }

  async getOrSet<T>(key: KeyInput, compute: () => T | Promise<T>, options?: SetOptions): Promise<T> {
    const hit = await this.get<T>(key);
    if (hit !== undefined) return hit;
    const value = await compute();
    await this.set(key, value, options);
    return value;
  }

  async many(keys: KeyInput[]): Promise<(unknown | undefined)[]> {
    const raws = await this.#store.many(await Promise.all(keys.map((k) => this.#physical(k))));
    return raws.map((raw) => this.#unwrap(raw));
  }

  async putMany(entries: [KeyInput, unknown][], options?: SetOptions): Promise<void> {
    for (const [key, value] of entries) await this.set(key, value, options);
  }

  async touch(key: KeyInput, ttl: number): Promise<boolean> {
    return this.#store.touch(await this.#physical(key), toMs(ttl));
  }

  /** Store only if absent. Returns true when stored. */
  async add(key: KeyInput, value: unknown, options?: SetOptions): Promise<boolean> {
    this.#assertSettable(value);
    const stored = await this.#store.add(await this.#physical(key), value, this.#ttlFor(value, options));
    if (stored) this.#emit({ type: "write", key: this.#full(key) });
    return stored;
  }

  async increment(key: KeyInput, by = 1): Promise<number> {
    return this.#store.increment(await this.#physical(key), by);
  }

  async decrement(key: KeyInput, by = 1): Promise<number> {
    return this.#store.decrement(await this.#physical(key), by);
  }

  async string(key: KeyInput, fallback?: string): Promise<string | undefined> {
    return this.#typed(key, (v): v is string => typeof v === "string", fallback);
  }

  async integer(key: KeyInput, fallback?: number): Promise<number | undefined> {
    return this.#typed(key, (v): v is number => Number.isInteger(v), fallback);
  }

  async float(key: KeyInput, fallback?: number): Promise<number | undefined> {
    return this.#typed(key, (v): v is number => typeof v === "number" && Number.isFinite(v), fallback);
  }

  async boolean(key: KeyInput, fallback?: boolean): Promise<boolean | undefined> {
    return this.#typed(key, (v): v is boolean => typeof v === "boolean", fallback);
  }

  async array(key: KeyInput, fallback?: unknown[]): Promise<unknown[] | undefined> {
    return this.#typed(key, (v): v is unknown[] => Array.isArray(v), fallback);
  }

  async #typed<T>(key: KeyInput, is: (v: unknown) => v is T, fallback: T | undefined): Promise<T | undefined> {
    const value = await this.get<unknown>(key);
    return value !== undefined && is(value) ? value : fallback;
  }

  /**
   * Stale-while-revalidate. `[fresh, stale]` ms: within `fresh` returns cached; within `stale` returns
   * the cached value and refreshes in the background; past that, computes synchronously.
   */
  async flexible<T>(key: KeyInput, [freshMs, staleMs]: [number, number], compute: () => T | Promise<T>): Promise<T> {
    const fresh = toMs(freshMs);
    const stale = toMs(staleMs);
    const stored = await this.#physical(key);
    const raw = await this.#store.get(stored);
    const now = this.#now();

    const refresh = async (): Promise<T> => {
      const value = await compute();
      this.#assertSettable(value);
      const envelope: FlexEnvelope = {
        [FLEX]: 1,
        v: value,
        freshUntil: this.#now() + fresh,
        staleUntil: this.#now() + stale,
      };
      await this.#store.put(stored, envelope, stale);
      this.#emit({ type: "write", key: this.#full(key) });
      return value;
    };

    if (isFlex(raw)) {
      if (now < raw.freshUntil) {
        this.#emit({ type: "hit", key: this.#full(key) });
        return raw.v as T;
      }
      if (now < raw.staleUntil) {
        this.#emit({ type: "hit", key: this.#full(key) });
        // Background refresh shares the in-flight run, so concurrent stale hits recompute once.
        // A failed refresh keeps serving the stale value rather than throwing.
        void this.#singleFlight(stored, refresh).catch(() => undefined);
        return raw.v as T;
      }
    }
    this.#emit({ type: "miss", key: this.#full(key) });
    return this.#singleFlight(stored, refresh);
  }

  /** Run `callback` while holding an exclusive lock on `key`. Waits up to `waitFor` ms, else LockTimeoutError. */
  async withoutOverlapping<T>(key: KeyInput, callback: () => T | Promise<T>, options: LockOptions = {}): Promise<T> {
    const lockKey = `lock:${this.#stored(key)}`;
    const token = `${Math.random()}:${this.#now()}`;
    await this.#acquire(lockKey, token, options, this.#full(key));
    try {
      return await callback();
    } finally {
      await this.#store.forgetIf(lockKey, token);
    }
  }

  /** At most `limit` callbacks run concurrently for `key`. Each slot is a lock with its own token. */
  async funnel<T>(key: KeyInput, options: FunnelOptions, callback: () => T | Promise<T>): Promise<T> {
    if (!Number.isInteger(options.limit) || options.limit < 1)
      throw new RangeError(`limit must be a positive integer, got ${options.limit}`);
    const deadline = this.#now() + (options.waitFor ?? 0);
    const base = `funnel:${this.#stored(key)}`;
    for (;;) {
      for (let slot = 0; slot < options.limit; slot++) {
        const slotKey = `${base}:${slot}`;
        const token = `${Math.random()}:${this.#now()}`;
        const got = await this.#store.add(slotKey, token, toMs(options.lockFor ?? 30_000));
        if (got) {
          try {
            return await callback();
          } finally {
            await this.#store.forgetIf(slotKey, token);
          }
        }
      }
      if (this.#now() >= deadline) throw new LockTimeoutError(this.#full(key));
      await sleepFor(10);
    }
  }

  async #acquire(lockKey: string, token: string, options: LockOptions, fullKey: Key): Promise<void> {
    const deadline = this.#now() + (options.waitFor ?? 0);
    const lockFor = toMs(options.lockFor ?? 30_000);
    for (;;) {
      if (await this.#store.add(lockKey, token, lockFor)) return;
      if (this.#now() >= deadline) throw new LockTimeoutError(fullKey);
      await sleepFor(10);
    }
  }

  async invalidate(filter: InvalidateFilter, options: InvalidateOptions = {}): Promise<number> {
    if (this.#scope.tags.length > 0) throw new UnsupportedError("invalidate()", "tagged caches; use flush()");
    const prefix = this.#scope.prefix;
    const relative = (key: Key): Key => key.slice(prefix.length);
    const matches = (key: Key): boolean => {
      if (!matchKey(prefix, key)) return false;
      if (typeof filter === "object" && !Array.isArray(filter) && "predicate" in filter) {
        return filter.predicate(relative(key));
      }
      return matchKey([...prefix, ...normalizeKey(filter as KeyInput)], key, options);
    };

    const doomed: { stored: string; key: Key }[] = [];
    for await (const { key: stored } of this.#store.entries()) {
      const key = decodeKey(stored);
      if (key && matches(key)) doomed.push({ stored, key });
    }
    let removed = 0;
    for (const { stored, key } of doomed) {
      if (await this.#store.forget(stored)) {
        removed++;
        this.#emit({ type: "forget", key });
      }
    }
    return removed;
  }

  async clear(): Promise<number> {
    return this.invalidate([]);
  }

  /** Underlying driver, read-only. */
  getStore(): Store {
    return this.#store;
  }
}
