import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import { afterAll, describe, expect, inject, it } from "vitest";
import { Cache, RedisStore } from "../src/index.ts";

// Memory and SQLite already reject an increment that leaves the safe-integer range. Redis must match.
const client = new Redis(inject("redisUrl"), { maxRetriesPerRequest: 1 });
const freshCache = () => new Cache(new RedisStore({ client, prefix: `int:${randomUUID()}:` }));

afterAll(() => {
  client.disconnect();
});

describe("RedisStore increment stays inside the safe-integer range", () => {
  it("rejects an increment that would pass MAX_SAFE_INTEGER", async () => {
    const cache = freshCache();
    await cache.set(["n"], Number.MAX_SAFE_INTEGER);
    await expect(cache.increment(["n"], 2)).rejects.toThrow(RangeError);
  });

  it("leaves the stored value unchanged after a rejected increment", async () => {
    const cache = freshCache();
    await cache.set(["n"], Number.MAX_SAFE_INTEGER);
    await cache.increment(["n"], 2).catch(() => undefined);
    expect(await cache.get(["n"])).toBe(Number.MAX_SAFE_INTEGER);
  });
});
