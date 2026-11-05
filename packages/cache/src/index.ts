// Core entry: no `effect`, no `vitest`. Redis driver needs `ioredis` at runtime only when you use it.
export type { Store } from "./contracts.ts";
export type { Key, KeyInput, Segment } from "./keys.ts";
export {
  Cache,
  LockTimeoutError,
  type CacheEvent,
  type CacheListener,
  type CacheOptions,
  type FunnelOptions,
  type InvalidateFilter,
  type InvalidateOptions,
  type LockOptions,
  type SetOptions,
} from "./cache.ts";
export { MemoryStore, type MemoryStoreOptions } from "./drivers/memory.ts";
export { SqliteStore, type SqliteStoreOptions } from "./drivers/sqlite.ts";
export { RedisStore, type RedisStoreOptions } from "./drivers/redis.ts";
export { UnsupportedError } from "./errors.ts";
export { isTaggable, type TaggableStore } from "./contracts.ts";
