import type { Store } from "../contracts.ts";

interface Entry {
  raw: string;
  expiresAt: number | null;
}

export interface MemoryStoreOptions {
  now?: () => number;
}

/**
 * Map for point lookups, plus `#sorted`: the same keys in code-unit order, so keysWithPrefix binary-searches.
 * Cost: a key insert or delete splices the sorted array, O(n) memmove. Reads and prefix scans are O(log n + matches).
 */
export class MemoryStore implements Store {
  readonly #entries = new Map<string, Entry>();
  readonly #sorted: string[] = [];
  readonly #now: () => number;

  constructor({ now = Date.now }: MemoryStoreOptions = {}) {
    this.#now = now;
  }

  /** First index whose key is >= `key`. */
  #lowerBound(key: string): number {
    let lo = 0;
    let hi = this.#sorted.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if ((this.#sorted[mid] as string) < key) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }

  #set(key: string, entry: Entry): void {
    if (!this.#entries.has(key)) this.#sorted.splice(this.#lowerBound(key), 0, key);
    this.#entries.set(key, entry);
  }

  #delete(key: string): void {
    if (!this.#entries.delete(key)) return;
    this.#sorted.splice(this.#lowerBound(key), 1);
  }

  #live(key: string): Entry | undefined {
    const entry = this.#entries.get(key);
    if (!entry) return undefined;
    if (entry.expiresAt !== null && this.#now() >= entry.expiresAt) {
      this.#delete(key);
      return undefined;
    }
    return entry;
  }

  #expiry(ttlMs: number | null): number | null {
    return ttlMs === null ? null : this.#now() + ttlMs;
  }

  async get(key: string): Promise<unknown> {
    const entry = this.#live(key);
    return entry ? JSON.parse(entry.raw) : undefined;
  }

  async many(keys: string[]): Promise<unknown[]> {
    return Promise.all(keys.map((key) => this.get(key)));
  }

  async put(key: string, value: unknown, ttlMs: number | null): Promise<void> {
    this.#set(key, { raw: JSON.stringify(value), expiresAt: this.#expiry(ttlMs) });
  }

  async putMany(values: Record<string, unknown>, ttlMs: number | null): Promise<void> {
    for (const [key, value] of Object.entries(values)) await this.put(key, value, ttlMs);
  }

  async add(key: string, value: unknown, ttlMs: number | null): Promise<boolean> {
    if (this.#live(key)) return false;
    await this.put(key, value, ttlMs);
    return true;
  }

  async increment(key: string, by = 1): Promise<number> {
    return this.#adjust(key, by);
  }

  async decrement(key: string, by = 1): Promise<number> {
    return this.#adjust(key, -by);
  }

  #adjust(key: string, by: number): number {
    const entry = this.#live(key);
    const current: unknown = entry ? JSON.parse(entry.raw) : 0;
    if (!Number.isInteger(current)) throw new TypeError(`Cache value at "${key}" is not an integer`);
    const next = (current as number) + by;
    this.#set(key, { raw: JSON.stringify(next), expiresAt: entry ? entry.expiresAt : null });
    return next;
  }

  async touch(key: string, ttlMs: number): Promise<boolean> {
    const entry = this.#live(key);
    if (!entry) return false;
    this.#set(key, { ...entry, expiresAt: this.#expiry(ttlMs) });
    return true;
  }

  async forget(key: string): Promise<boolean> {
    if (!this.#live(key)) return false;
    this.#delete(key);
    return true;
  }

  async forgetIf(key: string, expected: unknown): Promise<boolean> {
    const entry = this.#live(key);
    if (!entry || entry.raw !== JSON.stringify(expected)) return false;
    this.#delete(key);
    return true;
  }

  async extendIf(key: string, expected: unknown, ttlMs: number): Promise<boolean> {
    const entry = this.#live(key);
    if (!entry || entry.raw !== JSON.stringify(expected)) return false;
    this.#set(key, { ...entry, expiresAt: this.#expiry(ttlMs) });
    return true;
  }

  async *entries(): AsyncGenerator<{ key: string; value: unknown }> {
    for (const [key, entry] of this.#entries) {
      if (entry.expiresAt !== null && this.#now() >= entry.expiresAt) {
        this.#delete(key);
        continue;
      }
      yield { key, value: JSON.parse(entry.raw) };
    }
  }

  /**
   * Live keys starting with `prefix`, in sorted order. Expired keys met on the way are purged. Callers must not
   * mutate the store while iterating: Cache.invalidate collects keys first, then forgets them.
   */
  async *keysWithPrefix(prefix: string): AsyncGenerator<string> {
    for (let i = this.#lowerBound(prefix); i < this.#sorted.length; ) {
      const key = this.#sorted[i] as string;
      if (!key.startsWith(prefix)) return;
      const entry = this.#entries.get(key) as Entry;
      if (entry.expiresAt !== null && this.#now() >= entry.expiresAt) {
        this.#delete(key); // splices at i, so the next key lands at i
        continue;
      }
      i++;
      yield key;
    }
  }
}
