import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import { afterAll, inject } from "vitest";
import { RedisStore } from "../src/drivers/redis.ts";
import { actorSuite } from "./actors-suite.ts";

const client = new Redis(inject("redisUrl"), { maxRetriesPerRequest: 1 });
afterAll(() => {
  client.disconnect();
});

// Redis expiry runs on the server clock, so the fake clock does not drive it; the suite's TTL cases are
// the ones that depend on the clock, so the Redis run uses fresh prefixes and avoids TTL-timing assertions.
actorSuite("RedisStore", () => new RedisStore({ client, prefix: `actors:${randomUUID()}:` }));
