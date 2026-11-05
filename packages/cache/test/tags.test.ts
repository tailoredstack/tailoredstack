import { describe, it, expect, afterAll } from "vitest";
import { Redis } from "ioredis";
import { randomUUID } from "node:crypto";
import { Cache, MemoryStore, RedisStore, UnsupportedError, type TaggableStore } from "../src/index.ts";

// Tag tests need a live Redis/Valkey on 6390 (override with REDIS_URL). They fail loudly when it is down.
const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6390";
const client = new Redis(url, { maxRetriesPerRequest: 1 });

afterAll(() => {
  client.disconnect();
});

// Fresh store prefix per test, so tag ids never leak between tests.
const freshCache = () => new Cache(new RedisStore({ client, prefix: `test:${randomUUID()}:` }));
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe("tags: round-trip and isolation", () => {
  it("tagged set/get round-trips", async () => {
    const t = freshCache().tags(["user"]);
    await t.set(["profile", 1], { name: "a" });
    expect(await t.get(["profile", 1])).toEqual({ name: "a" });
  });

  it("tagged items are invisible to untagged reads", async () => {
    const cache = freshCache();
    await cache.tags(["user"]).set(["profile", 1], "x");
    expect(await cache.get(["profile", 1])).toBeUndefined();
  });

  it("tag order does not change the namespace", async () => {
    const cache = freshCache();
    await cache.tags(["a", "b"]).set(["k"], 1);
    expect(await cache.tags(["b", "a"]).get(["k"])).toBe(1);
  });

  it("tags([]) is rejected", () => {
    expect(() => freshCache().tags([])).toThrow(RangeError);
  });

  it("a tag name never collides with a plain user key", async () => {
    const cache = freshCache();
    await cache.set("tag:user", "plain");
    await cache.tags(["user"]).flush();
    expect(await cache.get("tag:user")).toBe("plain");
  });
});

describe("tags: flush", () => {
  it("flush makes tagged items unreachable, and writes after flush work", async () => {
    const t = freshCache().tags(["user"]);
    await t.set(["p", 1], "old");
    await t.flush();
    expect(await t.get(["p", 1])).toBeUndefined();
    await t.set(["p", 1], "new");
    expect(await t.get(["p", 1])).toBe("new");
  });

  it("flushing one tag leaves items under other tags", async () => {
    const cache = freshCache();
    await cache.tags(["user"]).set(["a"], "u");
    await cache.tags(["org:1"]).set(["b"], "o");
    await cache.tags(["user"]).flush();
    expect(await cache.tags(["org:1"]).get(["b"])).toBe("o");
  });

  it("flushing a tag also hides items tagged with it alongside other tags", async () => {
    const cache = freshCache();
    await cache.tags(["user", "org:1"]).set(["c"], "z");
    await cache.tags(["org:1"]).flush();
    expect(await cache.tags(["user", "org:1"]).get(["c"])).toBeUndefined();
  });

  it("untagged keys are unaffected by flush", async () => {
    const cache = freshCache();
    await cache.set(["u", 1], "plain");
    await cache.tags(["u"]).flush();
    expect(await cache.get(["u", 1])).toBe("plain");
  });

  it("flush() on an untagged cache is rejected", async () => {
    const untagged = freshCache() as unknown as { flush(): Promise<void> };
    await expect(untagged.flush()).rejects.toBeInstanceOf(UnsupportedError);
  });
});

describe("tags: expiry, sharing and scope", () => {
  it("expiry still applies to tagged items", async () => {
    const t = freshCache().tags(["user"]);
    await t.set(["p"], "v", { ttl: 300 });
    expect(await t.get(["p"])).toBe("v");
    await sleep(400);
    expect(await t.get(["p"])).toBeUndefined();
  });

  it("two Cache instances on the same store agree on tags", async () => {
    const prefix = `test:${randomUUID()}:`;
    const a = new Cache(new RedisStore({ client, prefix }));
    const b = new Cache(new RedisStore({ client, prefix }));
    await a.tags(["user"]).set(["p"], "from-a");
    expect(await b.tags(["user"]).get(["p"])).toBe("from-a");
    await b.tags(["user"]).flush();
    expect(await a.tags(["user"]).get(["p"])).toBeUndefined();
  });

  it("prefix() keeps the tag scope", async () => {
    const cache = freshCache();
    await cache.tags(["user"]).prefix(["profile"]).set([1], "x");
    expect(await cache.tags(["user"]).get(["profile", 1])).toBe("x");
  });

  it("invalidate() and clear() reject on tagged caches (use flush)", async () => {
    const t = freshCache().tags(["user"]);
    await expect(t.invalidate(["x"])).rejects.toBeInstanceOf(UnsupportedError);
    await expect(t.clear()).rejects.toBeInstanceOf(UnsupportedError);
  });

  it("base clear() does not remove tagged items", async () => {
    const cache = freshCache();
    await cache.tags(["user"]).set(["p"], 1);
    await cache.clear();
    expect(await cache.tags(["user"]).get(["p"])).toBe(1);
  });
});

describe("tags: runtime guard for untyped callers", () => {
  it("tags() on a non-taggable store throws UnsupportedError", () => {
    const untyped = new Cache(new MemoryStore()) as unknown as Cache<TaggableStore>;
    expect(() => untyped.tags(["user"])).toThrow(UnsupportedError);
  });
});
