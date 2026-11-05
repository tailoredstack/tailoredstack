/**
 * Driver primitives. TTLs are milliseconds; `null` means no expiry.
 * Values are JSON-serialized by the driver. Keys are opaque strings to the driver.
 */
export interface Store {
  get(key: string): Promise<unknown>;
  many(keys: string[]): Promise<unknown[]>;
  put(key: string, value: unknown, ttlMs: number | null): Promise<void>;
  putMany(values: Record<string, unknown>, ttlMs: number | null): Promise<void>;
  add(key: string, value: unknown, ttlMs: number | null): Promise<boolean>;
  increment(key: string, by?: number): Promise<number>;
  decrement(key: string, by?: number): Promise<number>;
  touch(key: string, ttlMs: number): Promise<boolean>;
  forget(key: string): Promise<boolean>;
  /** Delete only if the stored value equals `expected` (JSON-canonical). Used for lock release. */
  forgetIf(key: string, expected: unknown): Promise<boolean>;
  /**
   * Atomic compare-and-extend. If the live entry equals `expected` (JSON-canonical), set its expiry to now + ttlMs
   * and return true. Otherwise return false and change nothing. Never creates or revives a key. Used for lock renewal.
   */
  extendIf(key: string, expected: unknown, ttlMs: number): Promise<boolean>;
  /** Live (unexpired) entries only. */
  entries(): AsyncIterable<{ key: string; value: unknown }>;
  /**
   * Stored keys that start with `prefix` and are live. Cache.invalidate passes only encodeKey-shaped prefixes: an
   * exact key (ends in "]") or a children prefix (ends in ","). Must cost O(matches), not O(store).
   */
  keysWithPrefix(prefix: string): AsyncIterable<string>;
}
