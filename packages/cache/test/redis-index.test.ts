import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import { afterAll, describe, expect, inject, it } from "vitest";
import { Cache } from "../src/cache.ts";
import { RedisStore } from "../src/drivers/redis.ts";
import { encodeKey, indexPrefixesOf } from "../src/keys.ts";

const client = new Redis(inject("redisUrl"), { maxRetriesPerRequest: 1 });
const freshPrefix = () => `idx:${randomUUID()}:`;

afterAll(() => {
  client.disconnect();
});

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe("RedisStore prefix index", () => {
  it("writes index membership for every ancestor prefix of a key on set", async () => {
    const prefix = freshPrefix();
    const store = new RedisStore({ client, prefix });
    const stored = encodeKey(["user", 1, "posts"]);
    await store.put(stored, "p1", null);
    for (const child of indexPrefixesOf(stored)) {
      expect(await client.smembers(`${prefix}#idx:${child}`)).toContain(stored);
    }
  });

  it("writes index membership on add and increment", async () => {
    const prefix = freshPrefix();
    const store = new RedisStore({ client, prefix });
    const added = encodeKey(["jobs", "a"]);
    const counted = encodeKey(["jobs", "b"]);
    await store.add(added, 1, null);
    await store.increment(counted, 1);
    expect(await client.smembers(`${prefix}#idx:["jobs",`)).toEqual(expect.arrayContaining([added, counted]));
  });

  it("index sets are never returned by entries()", async () => {
    const prefix = freshPrefix();
    const store = new RedisStore({ client, prefix });
    await store.put(encodeKey(["user", 1]), 1, null);
    const seen: unknown[] = [];
    for await (const e of store.entries()) seen.push(e.value);
    expect(seen).toEqual([1]);
  });

  it("a TTL-expired member is dropped from the index and not counted by the next invalidate", async () => {
    const prefix = freshPrefix();
    const cache = new Cache(new RedisStore({ client, prefix }));
    await cache.set(["jobs", 1], "gone", { ttl: 300 });
    await cache.set(["jobs", 2], "kept");
    await sleep(600);
    expect(await cache.invalidate(["jobs"])).toBe(1);
    const members = await client.smembers(`${prefix}#idx:["jobs",`);
    expect(members).toEqual([encodeKey(["jobs", 2])]);
  });

  it("a TTL set with its index membership leaves no stale member after expiry plus invalidate", async () => {
    const prefix = freshPrefix();
    const store = new RedisStore({ client, prefix });
    const stored = encodeKey(["session", 9]);
    await store.put(stored, "s", 300);
    expect(await client.pttl(`${prefix}${stored}`)).toBeGreaterThan(0);
    expect(await client.smembers(`${prefix}#idx:["session",`)).toContain(stored);
    await sleep(600);
    const cache = new Cache(store);
    expect(await cache.invalidate(["session"])).toBe(0);
    expect(await client.smembers(`${prefix}#idx:["session",`)).toEqual([]);
  });
});
