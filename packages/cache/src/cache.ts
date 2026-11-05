import type { Store } from "./contracts.ts";
import { decodeKey, encodeKey, invalidationPrefixes, type Key, type KeyInput, matchKey, normalizeKey } from "./keys.ts";
import { assertSerializable } from "./values.ts";

export interface SetOptions {
  /** Milliseconds until expiry. A number, or a function of the value. Omit for the chained default; if none, no expiry. */
  ttl?: number | ((value: unknown) => number);
}

export type GetOrSetOptions = SetOptions & {
  /**
   * Single-flight the miss across processes via withoutOverlapping. Off by default. `true` uses the lock defaults;
   * a LockOptions object tunes them. Under the lock the key is re-read, so a value filled by another holder is used.
   */
  lock?: boolean | LockOptions;
};

export interface InvalidateOptions {
  exact?: boolean;
}

export type InvalidateFilter = KeyInput | { predicate: (key: Key) => boolean; exact?: never };

export interface CacheOptions {
  /** Clock for flexible() freshness windows. Defaults to Date.now. */
  now?: () => number;
}

export interface LockOptions {
  /**
   * Lease length (ms). A crashed holder's lock expires after this. While the callback runs, the lease is renewed
   * every lockFor/3 ms, so a live holder keeps exclusivity.
   */
  lockFor?: number;
  /**
   * Longest a holder keeps renewing (ms, default 300_000). Past this, renewal stops and the lock expires within
   * lockFor, so a callback that runs longer than maxHoldFor loses exclusivity.
   */
  maxHoldFor?: number;
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

// User objects with a reserved top-level key are wrapped on write so they never look like an envelope on read.
const ESCAPE = "__cache_escaped__";
interface EscapeEnvelope {
  [ESCAPE]: 1;
  v: unknown;
}

const isEscaped = (raw: unknown): raw is EscapeEnvelope =>
  typeof raw === "object" && raw !== null && (raw as Record<string, unknown>)[ESCAPE] === 1;

const needsEscape = (value: unknown): boolean =>
  typeof value === "object" && value !== null && (Object.hasOwn(value, FLEX) || Object.hasOwn(value, ESCAPE));

const escapeValue = (value: unknown): unknown => (needsEscape(value) ? { [ESCAPE]: 1, v: value } : value);
const unescapeValue = (raw: unknown): unknown => (isEscaped(raw) ? raw.v : raw);

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
    scope: Scope = { prefix: [], ttl: null },
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
    assertSerializable(value);
  }

  /** Unwraps flexible and escape envelopes; plain values pass through. */
  #unwrap(raw: unknown): unknown {
    return isFlex(raw) ? unescapeValue(raw.v) : unescapeValue(raw);
  }

  async get<T = unknown>(key: KeyInput): Promise<T | undefined> {
    const raw = await this.#store.get(this.#stored(key));
    if (raw === undefined) {
      this.#emit({ type: "miss", key: this.#full(key) });
      return undefined;
    }
    this.#emit({ type: "hit", key: this.#full(key) });
    return this.#unwrap(raw) as T;
  }

  async has(key: KeyInput): Promise<boolean> {
    return (await this.#store.get(this.#stored(key))) !== undefined;
  }

  async missing(key: KeyInput): Promise<boolean> {
    return !(await this.has(key));
  }

  async set(key: KeyInput, value: unknown, options?: SetOptions): Promise<void> {
    this.#assertSettable(value);
    await this.#store.put(this.#stored(key), escapeValue(value), this.#ttlFor(value, options));
    this.#emit({ type: "write", key: this.#full(key) });
  }

  async delete(key: KeyInput): Promise<boolean> {
    const removed = await this.#store.forget(this.#stored(key));
    if (removed) this.#emit({ type: "forget", key: this.#full(key) });
    return removed;
  }

  async deleteMultiple(keys: KeyInput[]): Promise<number> {
    let removed = 0;
    for (const key of keys) if (await this.delete(key)) removed++;
    return removed;
  }

  async getOrSet<T>(key: KeyInput, compute: () => T | Promise<T>, options?: GetOrSetOptions): Promise<T> {
    const hit = await this.get<T>(key);
    if (hit !== undefined) return hit;
    const { lock, ...setOptions } = options ?? {};
    const fill = async (): Promise<T> => {
      const value = await compute();
      await this.set(key, value, setOptions);
      return value;
    };
    if (!lock) return fill();
    return this.withoutOverlapping(key, async () => (await this.get<T>(key)) ?? fill(), lock === true ? {} : lock);
  }

  async many(keys: KeyInput[]): Promise<(unknown | undefined)[]> {
    const raws = await this.#store.many(keys.map((k) => this.#stored(k)));
    return raws.map((raw) => this.#unwrap(raw));
  }

  async putMany(entries: [KeyInput, unknown][], options?: SetOptions): Promise<void> {
    // Validate the whole batch first so a bad entry does not leave earlier entries written.
    for (const [, value] of entries) this.#assertSettable(value);
    for (const [key, value] of entries) await this.set(key, value, options);
  }

  async touch(key: KeyInput, ttl: number): Promise<boolean> {
    return this.#store.touch(this.#stored(key), toMs(ttl));
  }

  /** Store only if absent. Returns true when stored. */
  async add(key: KeyInput, value: unknown, options?: SetOptions): Promise<boolean> {
    this.#assertSettable(value);
    const stored = await this.#store.add(this.#stored(key), escapeValue(value), this.#ttlFor(value, options));
    if (stored) this.#emit({ type: "write", key: this.#full(key) });
    return stored;
  }

  #assertStep(by: number): void {
    if (!Number.isSafeInteger(by)) throw new RangeError(`step must be a safe integer, got ${by}`);
  }

  async increment(key: KeyInput, by = 1): Promise<number> {
    this.#assertStep(by);
    return this.#store.increment(this.#stored(key), by);
  }

  async decrement(key: KeyInput, by = 1): Promise<number> {
    this.#assertStep(by);
    return this.#store.decrement(this.#stored(key), by);
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
    if (fresh > stale)
      throw new RangeError(`flexible() fresh window (${freshMs}ms) must not exceed stale (${staleMs}ms)`);
    const stored = this.#stored(key);
    const raw = await this.#store.get(stored);
    const now = this.#now();

    const refresh = async (): Promise<T> => {
      const value = await compute();
      this.#assertSettable(value);
      const envelope: FlexEnvelope = {
        [FLEX]: 1,
        v: escapeValue(value),
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
        return unescapeValue(raw.v) as T;
      }
      if (now < raw.staleUntil) {
        this.#emit({ type: "hit", key: this.#full(key) });
        // Background refresh shares the in-flight run, so concurrent stale hits recompute once.
        // A failed refresh keeps serving the stale value rather than throwing.
        void this.#singleFlight(stored, refresh).catch(() => undefined);
        return unescapeValue(raw.v) as T;
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
    return this.#lease(lockKey, token, options, callback);
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
        if (got) return this.#lease(slotKey, token, options, callback);
      }
      if (this.#now() >= deadline) throw new LockTimeoutError(this.#full(key));
      await sleepFor(10);
    }
  }

  /**
   * Run `callback` under a held lock, renewing the lease every lockFor/3 until the callback settles or maxHoldFor
   * passes. A lost lease (extendIf false) or a failed renewal stops renewals without failing the callback.
   * Release is token-checked, so a late release never frees a successor's lock.
   */
  async #lease<T>(lockKey: string, token: string, options: LockOptions, callback: () => T | Promise<T>): Promise<T> {
    const lockFor = toMs(options.lockFor ?? 30_000);
    const maxHoldFor = toMs(options.maxHoldFor ?? 300_000);
    const started = this.#now();
    let active = true;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const renew = async (): Promise<void> => {
      if (!active) return;
      if (this.#now() - started >= maxHoldFor) {
        active = false;
        return;
      }
      try {
        if (!(await this.#store.extendIf(lockKey, token, lockFor))) active = false;
      } catch {
        active = false;
      }
      if (active) timer = setTimeout(renew, lockFor / 3);
    };
    timer = setTimeout(renew, lockFor / 3);
    try {
      return await callback();
    } finally {
      active = false;
      clearTimeout(timer);
      await this.#store.forgetIf(lockKey, token);
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
    const prefix = this.#scope.prefix;
    const doomed: { stored: string; key: Key }[] = [];
    if (typeof filter === "object" && !Array.isArray(filter) && "predicate" in filter) {
      // Predicate filters cannot use the prefix index: full scan of the store (documented).
      const relative = (key: Key): Key => key.slice(prefix.length);
      for await (const { key: stored } of this.#store.entries()) {
        const key = decodeKey(stored);
        if (key && matchKey(prefix, key) && filter.predicate(relative(key))) doomed.push({ stored, key });
      }
    } else {
      // Array filters: visit only the index prefixes that cover the matches (O(matches)). matchKey still decides.
      const full = [...prefix, ...normalizeKey(filter as KeyInput)];
      for (const indexPrefix of invalidationPrefixes(full, options.exact === true)) {
        for await (const stored of this.#store.keysWithPrefix(indexPrefix)) {
          const key = decodeKey(stored);
          if (key && matchKey(full, key, options)) doomed.push({ stored, key });
        }
      }
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
  getStore(): S {
    return this.#store;
  }
}
