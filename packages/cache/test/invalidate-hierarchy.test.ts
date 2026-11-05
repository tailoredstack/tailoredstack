import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Redis } from "ioredis";
import { afterAll, describe, expect, inject, it } from "vitest";
import { Cache, MemoryStore, RedisStore, SqliteStore, type Store } from "../src/index.ts";

// Array keys are hierarchical. invalidate() is a partial-match scan, and it must behave the same on every store.
const client = new Redis(inject("redisUrl"), { maxRetriesPerRequest: 1 });
const dir = mkdtempSync(join(tmpdir(), "cache-hier-"));

afterAll(() => {
  client.disconnect();
  rmSync(dir, { recursive: true, force: true });
});

const stores: [string, () => Store][] = [
  ["memory", () => new MemoryStore()],
  ["sqlite", () => new SqliteStore({ path: join(dir, `${randomUUID()}.db`) })],
  ["redis", () => new RedisStore({ client, prefix: `hier:${randomUUID()}:` })],
];

describe.each(stores)("array-key invalidation on %s", (_name, make) => {
  it("partial invalidate removes every key under the prefix and keeps look-alike segments", async () => {
    const cache = new Cache(make());
    await cache.set(["user", 1], "u1");
    await cache.set(["user", 1, "posts"], "p1");
    await cache.set(["username", 1], "keep");
    expect(await cache.invalidate(["user"])).toBe(2);
    expect(await cache.get(["user", 1])).toBeUndefined();
    expect(await cache.get(["user", 1, "posts"])).toBeUndefined();
    expect(await cache.get(["username", 1])).toBe("keep");
  });

  it("exact invalidate removes only the exact key", async () => {
    const cache = new Cache(make());
    await cache.set(["user", 1], "u1");
    await cache.set(["user", 1, "posts"], "p1");
    expect(await cache.invalidate(["user", 1], { exact: true })).toBe(1);
    expect(await cache.get(["user", 1])).toBeUndefined();
    expect(await cache.get(["user", 1, "posts"])).toBe("p1");
  });

  it("a prefixed cache invalidates relative to its prefix", async () => {
    const base = new Cache(make());
    await base.set(["user", 1], "u1");
    await base.set(["org", 1], "o1");
    await base.prefix(["user"]).invalidate([1]);
    expect(await base.get(["user", 1])).toBeUndefined();
    expect(await base.get(["org", 1])).toBe("o1");
  });

  it("invalidate with an object segment matches partially", async () => {
    const cache = new Cache(make());
    await cache.set(["list", { page: 1, sort: "a" }], "p1");
    await cache.set(["list", { page: 2, sort: "a" }], "p2");
    expect(await cache.invalidate(["list", { page: 1 }])).toBe(1);
    expect(await cache.get(["list", { page: 2, sort: "a" }])).toBe("p2");
  });
});
