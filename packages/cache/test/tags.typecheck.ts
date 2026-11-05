// Compile-time gate for tags(). Checked by `pnpm typecheck` (tsc), not by vitest.
import { Redis } from "ioredis";
import { Cache, MemoryStore, RedisStore } from "../src/index.ts";

const memory = new Cache(new MemoryStore());
// @ts-expect-error MemoryStore is not a TaggableStore, so tags() must not compile
memory.tags(["user"]);

const redis = new Cache(new RedisStore({ client: new Redis() }));
redis.tags(["user"]);

// tags on set/getOrSet must only compile for a taggable store.
const plain = new Cache(new MemoryStore());
// @ts-expect-error MemoryStore is not a TaggableStore, so set() must reject tags
plain.set("k", 1, { tags: ["user"] });
// @ts-expect-error MemoryStore is not a TaggableStore, so getOrSet() must reject tags
plain.getOrSet("k", () => 1, { tags: ["user"] });
redis.set("k", 1, { tags: ["user"] });
redis.getOrSet("k", () => 1, { tags: ["user"] });
