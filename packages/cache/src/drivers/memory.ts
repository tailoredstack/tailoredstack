import type { Store } from "../contracts.ts";

interface Entry {
  raw: string;
  expiresAt: number | null;
}

export interface MemoryStoreOptions {
  now?: () => number;
}

export class MemoryStore implements Store {
  readonly #entries = new Map<string, Entry>();
  readonly #now: () => number;

  constructor({ now = Date.now }: MemoryStoreOptions = {}) {
    this.#now = now;
  }

  #live(key: string): Entry | undefined {
    const entry = this.#entries.get(key);
    if (!entry) return undefined;
    if (entry.expiresAt !== null && this.#now() >= entry.expiresAt) {
      this.#entries.delete(key);
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
    this.#entries.set(key, { raw: JSON.stringify(value), expiresAt: this.#expiry(ttlMs) });
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
    this.#entries.set(key, { raw: JSON.stringify(next), expiresAt: entry ? entry.expiresAt : null });
    return next;
  }

  async touch(key: string, ttlMs: number): Promise<boolean> {
    const entry = this.#live(key);
    if (!entry) return false;
    this.#entries.set(key, { ...entry, expiresAt: this.#expiry(ttlMs) });
    return true;
  }

  async forget(key: string): Promise<boolean> {
    if (!this.#live(key)) return false;
    this.#entries.delete(key);
    return true;
  }

  async forgetIf(key: string, expected: unknown): Promise<boolean> {
    const entry = this.#live(key);
    if (!entry || entry.raw !== JSON.stringify(expected)) return false;
    this.#entries.delete(key);
    return true;
  }

  async extendIf(key: string, expected: unknown, ttlMs: number): Promise<boolean> {
    const entry = this.#live(key);
    if (!entry || entry.raw !== JSON.stringify(expected)) return false;
    this.#entries.set(key, { ...entry, expiresAt: this.#expiry(ttlMs) });
    return true;
  }

  async *entries(): AsyncGenerator<{ key: string; value: unknown }> {
    for (const [key, entry] of this.#entries) {
      if (entry.expiresAt !== null && this.#now() >= entry.expiresAt) {
        this.#entries.delete(key);
        continue;
      }
      yield { key, value: JSON.parse(entry.raw) };
    }
  }
}
