import { describe, it, expect, afterAll } from "vitest";
import { Redis } from "ioredis";
import { randomUUID } from "node:crypto";
import { RedisStore } from "../src/drivers/redis.ts";
import { storeContract } from "../src/testing/index.ts";

// Local Redis/Valkey on 6390 (see README). Override with REDIS_URL.
const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6390";
const client = new Redis(url, { maxRetriesPerRequest: 1 });

// Each store gets a unique key prefix so tests never see each other's keys.
const freshStore = () => new RedisStore({ client, prefix: `test:${randomUUID()}:` });

afterAll(async () => {
  client.disconnect();
});

// Redis expiry runs on the server clock, so time-travel tests are skipped; expiry is tested with real waits below.
storeContract("RedisStore", () => freshStore(), { timeTravel: false });

describe("RedisStore real-time behavior", () => {
  it("expires a key after its TTL", async () => {
    const store = freshStore();
    await store.put("k", "v", 1000);
    expect(await store.get("k")).toBe("v");
    await new Promise((r) => setTimeout(r, 1100));
    expect(await store.get("k")).toBeUndefined();
  });

  it("add succeeds again after the existing key expires", async () => {
    const store = freshStore();
    await store.put("k", "old", 1000);
    await new Promise((r) => setTimeout(r, 1100));
    expect(await store.add("k", "new", null)).toBe(true);
    expect(await store.get("k")).toBe("new");
  });

  it("increment keeps the TTL set by put", async () => {
    const store = freshStore();
    await store.put("n", 1, 1000);
    await store.increment("n", 1);
    await new Promise((r) => setTimeout(r, 1100));
    expect(await store.get("n")).toBeUndefined();
  });

  it("entries only yields keys under its own store prefix", async () => {
    const a = freshStore();
    const b = freshStore();
    await a.put("k", 1, null);
    await b.put("k", 2, null);
    const seen: unknown[] = [];
    for await (const e of a.entries()) seen.push(e.value);
    expect(seen).toEqual([1]);
  });

  it("treats glob characters in a store prefix literally", async () => {
    const base = `test:${randomUUID()}:a*b:`;
    const store = new RedisStore({ client, prefix: base });
    const other = new RedisStore({ client, prefix: base.replace("a*b", "axb") });
    await store.put("1", 1, null);
    await other.put("1", 2, null);
    const seen: unknown[] = [];
    for await (const e of store.entries()) seen.push(e.value);
    expect(seen).toEqual([1]);
  });
});
