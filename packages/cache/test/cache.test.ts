import { describe, it, expect } from "vitest";
import { Cache } from "../src/cache.ts";
import { MemoryStore } from "../src/drivers/memory.ts";
import { makeClock } from "../src/testing/index.ts";

function setup() {
  const clock = makeClock();
  const store = new MemoryStore({ now: clock.now });
  return { clock, store, cache: new Cache(store) };
}

describe("Cache basic API", () => {
  it("get returns undefined on a miss", async () => {
    const { cache } = setup();
    expect(await cache.get(["nope"])).toBeUndefined();
  });

  it("set/get round-trips typed values", async () => {
    const { cache } = setup();
    await cache.set(["user", 1], { id: 1 });
    expect(await cache.get<{ id: number }>(["user", 1])).toEqual({ id: 1 });
  });

  it("a string key is the same as a one-segment array", async () => {
    const { cache } = setup();
    await cache.set("config", 1);
    expect(await cache.get(["config"])).toBe(1);
  });

  it("stored null is returned as null, not a miss", async () => {
    const { cache } = setup();
    await cache.set(["k"], null);
    expect(await cache.get(["k"])).toBeNull();
  });

  it("set without ttl never expires", async () => {
    const { clock, cache } = setup();
    await cache.set(["k"], "v");
    clock.advance(315_360_000_000);
    expect(await cache.get(["k"])).toBe("v");
  });

  it("set with ttl expires after ms", async () => {
    const { clock, cache } = setup();
    await cache.set(["k"], "v", { ttl: 1000 });
    clock.advance(999);
    expect(await cache.get(["k"])).toBe("v");
    clock.advance(1);
    expect(await cache.get(["k"])).toBeUndefined();
  });

  it("has and delete report presence", async () => {
    const { cache } = setup();
    expect(await cache.has(["k"])).toBe(false);
    await cache.set(["k"], 0);
    expect(await cache.has(["k"])).toBe(true);
    expect(await cache.delete(["k"])).toBe(true);
    expect(await cache.delete(["k"])).toBe(false);
  });

  it("getOrSet computes once and caches the result", async () => {
    const { cache } = setup();
    let calls = 0;
    const compute = () => ++calls;
    expect(await cache.getOrSet(["k"], compute)).toBe(1);
    expect(await cache.getOrSet(["k"], compute)).toBe(1);
    expect(calls).toBe(1);
  });

  it("getOrSet returns a cached null without recomputing", async () => {
    const { cache } = setup();
    let calls = 0;
    await cache.set(["k"], null);
    expect(
      await cache.getOrSet(["k"], () => {
        calls++;
        return "x";
      }),
    ).toBeNull();
    expect(calls).toBe(0);
  });

  it("increment and decrement return the new value", async () => {
    const { cache } = setup();
    expect(await cache.increment(["n"], 2)).toBe(2);
    expect(await cache.decrement(["n"], 1)).toBe(1);
  });

  it("the same key array in a different object key order is the same entry", async () => {
    const { cache } = setup();
    await cache.set(["u", { a: 1, b: 2 }], "x");
    expect(await cache.get(["u", { b: 2, a: 1 }])).toBe("x");
  });
});

describe("Cache.invalidate (TanStack partial match)", () => {
  it("invalidates every key that starts with the filter", async () => {
    const { cache } = setup();
    await cache.set(["user", 1, "profile"], "p1");
    await cache.set(["user", 2], "u2");
    await cache.set(["session", 1], "s1");
    expect(await cache.invalidate(["user"])).toBe(2);
    expect(await cache.has(["user", 1, "profile"])).toBe(false);
    expect(await cache.has(["user", 2])).toBe(false);
    expect(await cache.get(["session", 1])).toBe("s1");
  });

  it("does not invalidate keys whose segment only starts with the filter text", async () => {
    const { cache } = setup();
    await cache.set(["username", 1], "x");
    await cache.invalidate(["user"]);
    expect(await cache.get(["username", 1])).toBe("x");
  });

  it("partially matches object segments", async () => {
    const { cache } = setup();
    await cache.set(["user", { page: 1, sort: "a" }], "p1");
    await cache.set(["user", { page: 2, sort: "a" }], "p2");
    await cache.invalidate(["user", { page: 1 }]);
    expect(await cache.has(["user", { page: 1, sort: "a" }])).toBe(false);
    expect(await cache.has(["user", { page: 2, sort: "a" }])).toBe(true);
  });

  it("exact: true only removes the exact key", async () => {
    const { cache } = setup();
    await cache.set(["user", 1], "u");
    await cache.set(["user", 1, "posts"], "p");
    expect(await cache.invalidate(["user", 1], { exact: true })).toBe(1);
    expect(await cache.has(["user", 1])).toBe(false);
    expect(await cache.has(["user", 1, "posts"])).toBe(true);
  });

  it("predicate receives the decoded key array", async () => {
    const { cache } = setup();
    await cache.set(["user", 1], "u");
    await cache.set(["post", 1], "p");
    const removed = await cache.invalidate({ predicate: (key) => key[0] === "post" });
    expect(removed).toBe(1);
    expect(await cache.get(["user", 1])).toBe("u");
  });

  it("invalidate([]) removes everything; clear() is the same", async () => {
    const { cache } = setup();
    await cache.set(["a"], 1);
    await cache.set(["b", 2], 2);
    await cache.clear();
    expect(await cache.has(["a"])).toBe(false);
    expect(await cache.has(["b", 2])).toBe(false);
  });

  it("expired entries are not counted or removed by invalidate", async () => {
    const { clock, cache } = setup();
    await cache.set(["user", 1], "u", { ttl: 1000 });
    clock.advance(1000);
    expect(await cache.invalidate(["user"])).toBe(0);
  });
});

describe("Cache fluent API", () => {
  it("prefix prepends key segments without touching the base cache", async () => {
    const { cache } = setup();
    const users = cache.prefix(["user"]);
    await users.set([1], "alice");
    expect(await cache.get(["user", 1])).toBe("alice");
    expect(await cache.get([1])).toBeUndefined();
    expect(await users.get([1])).toBe("alice");
  });

  it("a string prefix is a single segment", async () => {
    const { cache } = setup();
    await cache.prefix("user").set([1], "alice");
    expect(await cache.get(["user", 1])).toBe("alice");
  });

  it("prefixes nest", async () => {
    const { cache } = setup();
    await cache.prefix(["a"]).prefix(["b"]).set(["k"], 1);
    expect(await cache.get(["a", "b", "k"])).toBe(1);
  });

  it("returns new instances; chains do not mutate each other", async () => {
    const { cache } = setup();
    const a = cache.prefix(["a"]);
    const b = cache.prefix(["b"]);
    expect(a).not.toBe(cache);
    await a.set(["k"], 1);
    await b.set(["k"], 2);
    expect(await cache.get(["a", "k"])).toBe(1);
    expect(await cache.get(["b", "k"])).toBe(2);
  });

  it("invalidate on a prefixed cache is relative to the prefix", async () => {
    const { cache } = setup();
    await cache.set(["user", 1], "u");
    await cache.set(["session", 1], "s");
    await cache.prefix(["user"]).invalidate([1]);
    expect(await cache.has(["user", 1])).toBe(false);
    expect(await cache.get(["session", 1])).toBe("s");
  });

  it("invalidate([]) on a prefixed cache removes only that prefix", async () => {
    const { cache } = setup();
    await cache.set(["a"], 1);
    await cache.prefix(["user"]).set([1], 2);
    await cache.prefix(["user"]).clear();
    expect(await cache.get(["a"])).toBe(1);
    expect(await cache.get(["user", 1])).toBeUndefined();
  });

  it("ttl sets the default expiry for set()", async () => {
    const { clock, cache } = setup();
    await cache.ttl(1000).set(["k"], "v");
    clock.advance(1000);
    expect(await cache.get(["k"])).toBeUndefined();
  });

  it("the base cache keeps forever-expiry after a chained ttl", async () => {
    const { clock, cache } = setup();
    cache.ttl(1000);
    await cache.set(["k"], "v");
    clock.advance(315_360_000_000);
    expect(await cache.get(["k"])).toBe("v");
  });

  it("per-call ttl overrides the chained default", async () => {
    const { clock, cache } = setup();
    await cache.ttl(1000).set(["k"], "v", { ttl: 5000 });
    clock.advance(1000);
    expect(await cache.get(["k"])).toBe("v");
  });

  it("prefix and ttl compose", async () => {
    const { clock, cache } = setup();
    const users = cache.prefix(["user"]).ttl(1000);
    await users.set([1], "alice");
    expect(await cache.get(["user", 1])).toBe("alice");
    clock.advance(1000);
    expect(await cache.get(["user", 1])).toBeUndefined();
  });

  it("ttl rejects non-positive or non-finite values", () => {
    const { cache } = setup();
    expect(() => cache.ttl(0)).toThrow(RangeError);
    expect(() => cache.ttl(-1)).toThrow(RangeError);
    expect(() => cache.ttl(Number.NaN)).toThrow(RangeError);
  });

  it("invalid key segments throw TypeError at every entry point", async () => {
    const { cache } = setup();
    await expect(cache.set(["u", undefined as never], 1)).rejects.toThrow(TypeError);
    await expect(cache.prefix(["u"]).get([Number.NaN])).rejects.toThrow(TypeError);
  });
});
