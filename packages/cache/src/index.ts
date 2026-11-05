// Core entry: no `effect`, no `vitest`. Redis driver needs `ioredis` at runtime only when you use it.

export {
  Cache,
  type CacheEvent,
  type CacheListener,
  type CacheOptions,
  type FunnelOptions,
  type GetOrSetOptions,
  type InvalidateFilter,
  type InvalidateOptions,
  type LockOptions,
  LockTimeoutError,
  type SetOptions,
} from "./cache.ts";
export type { Store } from "./contracts.ts";
export { isTaggable, type TaggableStore } from "./contracts.ts";
export { MemoryStore, type MemoryStoreOptions } from "./drivers/memory.ts";
export { RedisStore, type RedisStoreOptions } from "./drivers/redis.ts";
export { SqliteStore, type SqliteStoreOptions } from "./drivers/sqlite.ts";
export { UnsupportedError } from "./errors.ts";
export type { Key, KeyInput, Segment } from "./keys.ts";
