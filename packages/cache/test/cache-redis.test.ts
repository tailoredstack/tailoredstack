import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import { afterAll, describe, expect, inject, it } from "vitest";
import { Cache, RedisStore } from "../src/index.ts";

// Redis container started once per run by test/global-setup.ts.
const client = new Redis(inject("redisUrl"), { maxRetriesPerRequest: 1 });
const freshCache = () => new Cache(new RedisStore({ client, prefix: `test:${randomUUID()}:` }));

afterAll(() => {
  client.disconnect();
});

describe("Cache on RedisStore: fractional ttl", () => {
  it("rounds a fractional ttl up to whole milliseconds instead of throwing", async () => {
    const cache = freshCache();
    await cache.set(["k"], "v", { ttl: 1500.5 });
    expect(await cache.get(["k"])).toBe("v");
  });

  it("touch accepts a fractional ttl", async () => {
    const cache = freshCache();
    await cache.set(["k"], "v", { ttl: 1000 });
    expect(await cache.touch(["k"], 2500.5)).toBe(true);
  });
});

describe("Cache on RedisStore: fractional lock ttl", () => {
  it("withoutOverlapping accepts a fractional lockFor", async () => {
    const cache = freshCache();
    expect(await cache.withoutOverlapping(["job"], () => "ok", { lockFor: 1500.5, waitFor: 0 })).toBe("ok");
  });
});
