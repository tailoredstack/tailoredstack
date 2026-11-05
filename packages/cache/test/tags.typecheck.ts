// Compile-time gate for tags(). Checked by `pnpm typecheck` (tsc), not by vitest.
import { Redis } from "ioredis";
import { Cache, MemoryStore, RedisStore } from "../src/index.ts";

const memory = new Cache(new MemoryStore());
// @ts-expect-error MemoryStore is not a TaggableStore, so tags() must not compile
memory.tags(["user"]);

const redis = new Cache(new RedisStore({ client: new Redis() }));
redis.tags(["user"]);
