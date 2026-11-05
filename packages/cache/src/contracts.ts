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
  /** Live (unexpired) entries only. */
  entries(): AsyncIterable<{ key: string; value: unknown }>;
}

/**
 * Optional capability for drivers with native O(1) tag invalidation.
 * A tag is a versioned id; items are stored under a key that embeds the current ids,
 * so rotating an id makes every item under it unreachable (they expire on their own TTL).
 */
export interface TaggableStore extends Store {
  /** Current id for `name`, creating one if absent. Ids never expire. */
  tagId(name: string): Promise<string>;
  /** Replace the id for `name`. Items under the old id become unreachable. */
  rotateTag(name: string): Promise<void>;
}

export function isTaggable(store: Store): store is TaggableStore {
  return (
    typeof (store as Partial<TaggableStore>).tagId === "function" &&
    typeof (store as Partial<TaggableStore>).rotateTag === "function"
  );
}
