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

// Wraps a store and counts entries() calls. Methods are bound so private fields keep working.
function counting(store: Store) {
  const counter = { entries: 0 };
  const proxy = new Proxy(store, {
    get(target, prop) {
      if (prop === "entries") {
        return () => {
          counter.entries++;
          return target.entries();
        };
      }
      const value = Reflect.get(target, prop, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  return { store: proxy, counter };
}

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

  it("exact invalidate with an object segment matches only the full key", async () => {
    const cache = new Cache(make());
    await cache.set(["list", { page: 1, sort: "a" }], "full");
    await cache.set(["list", { page: 1 }], "partial");
    expect(await cache.invalidate(["list", { page: 1 }], { exact: true })).toBe(1);
    expect(await cache.get(["list", { page: 1, sort: "a" }])).toBe("full");
    expect(await cache.get(["list", { page: 1 }])).toBeUndefined();
  });

  it("object first segment matches partially across the whole store", async () => {
    const cache = new Cache(make());
    await cache.set([{ tenant: "a" }, 1], "a1");
    await cache.set([{ tenant: "a" }, 2], "a2");
    await cache.set([{ tenant: "b" }, 1], "b1");
    expect(await cache.invalidate([{ tenant: "a" }])).toBe(2);
    expect(await cache.get([{ tenant: "b" }, 1])).toBe("b1");
  });

  it("nested object segments match partially at every depth", async () => {
    const cache = new Cache(make());
    await cache.set(["list", { filter: { role: "admin", active: true } }], "admin");
    await cache.set(["list", { filter: { role: "user" } }], "user");
    expect(await cache.invalidate(["list", { filter: { role: "admin" } }])).toBe(1);
    expect(await cache.get(["list", { filter: { role: "user" } }])).toBe("user");
  });

  it("object segment under a literal prefix keeps sibling ids", async () => {
    const cache = new Cache(make());
    await cache.set(["user", { id: 1 }, "posts"], "p1");
    await cache.set(["user", { id: 2 }, "posts"], "p2");
    expect(await cache.invalidate(["user", { id: 1 }])).toBe(1);
    expect(await cache.get(["user", { id: 2 }, "posts"])).toBe("p2");
  });

  it("clear removes every key of the store", async () => {
    const cache = new Cache(make());
    await cache.set(["a", 1], 1);
    await cache.set(["b", { x: 1 }], 2);
    expect(await cache.clear()).toBe(2);
    expect(await cache.get(["a", 1])).toBeUndefined();
  });
});

describe.each(stores)("array-key invalidation cost on %s", (_name, make) => {
  it("partial array-key invalidate never calls entries()", async () => {
    const { store, counter } = counting(make());
    const cache = new Cache(store);
    await cache.set(["user", 1], "u1");
    await cache.set(["user", 1, "posts"], "p1");
    await cache.set(["username", 1], "keep");
    expect(await cache.invalidate(["user"])).toBe(2);
    expect(counter.entries).toBe(0);
  });

  it("exact array-key invalidate never calls entries()", async () => {
    const { store, counter } = counting(make());
    const cache = new Cache(store);
    await cache.set(["user", 1], "u1");
    expect(await cache.invalidate(["user", 1], { exact: true })).toBe(1);
    expect(counter.entries).toBe(0);
  });

  // Documented full scan: a predicate filter cannot use the prefix index, so it is allowed to call entries().
  it("predicate invalidate (documented full scan) still removes matches via entries()", async () => {
    const { store, counter } = counting(make());
    const cache = new Cache(store);
    await cache.set(["user", 1], "u1");
    await cache.set(["org", 1], "o1");
    expect(await cache.invalidate({ predicate: (key) => key[0] === "user" })).toBe(1);
    expect(counter.entries).toBeGreaterThan(0);
    expect(await cache.get(["org", 1])).toBe("o1");
  });
});
