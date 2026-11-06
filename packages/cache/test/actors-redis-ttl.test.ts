import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import { afterAll, describe, expect, inject, it } from "vitest";
import { Cache, RedisStore } from "../src/index.ts";

// Redis expiry runs on the server clock, so these use short real TTLs. Covers the ttl paths the shared actor
// suite cannot reach with a fake clock.
const client = new Redis(inject("redisUrl"), { maxRetriesPerRequest: 1 });
afterAll(() => {
  client.disconnect();
});

const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));
const freshCache = () => new Cache(new RedisStore({ client, prefix: `ttl:${randomUUID()}:` }));

describe("RedisStore ttl: real server-clock expiry", () => {
  it("a ttl function returning a fractional ms is rounded up and the value expires", async () => {
    const cache = freshCache();
    await cache.set(["f"], "v", { ttl: () => 200.4 });
    expect(await cache.get(["f"])).toBe("v");
    await wait(400);
    expect(await cache.get(["f"])).toBeUndefined();
  });

  it("a per-call ttl overrides the chained ttl on redis", async () => {
    const cache = freshCache();
    await cache.ttl(60_000).set(["o"], "v", { ttl: 200 });
    await wait(400);
    expect(await cache.get(["o"])).toBeUndefined();
  });

  it("an expired key is absent from prefix invalidate and entries on redis", async () => {
    const cache = freshCache();
    await cache.set(["u", 1], "a", { ttl: 200 });
    await cache.set(["u", 2], "b");
    await wait(400);
    expect(await cache.invalidate(["u"])).toBe(1);
    expect(await cache.get(["u", 2])).toBeUndefined();
  });
});
