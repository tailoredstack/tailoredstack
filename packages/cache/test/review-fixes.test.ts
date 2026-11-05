import { describe, expect, it } from "vitest";
import { Cache, MemoryStore } from "../src/index.ts";

// Class instances with a prototype toJSON are what JSON.stringify actually serializes, so these tests use them.
// A plain object with a function-valued toJSON key is rejected by the guard for an unrelated reason.

describe("escape marker cannot be forged through toJSON", () => {
  it("a class whose toJSON yields the escape shape does not read back as the inner value", async () => {
    class Forged {
      toJSON() {
        return { __cache_escaped__: 1, v: "LEAK" };
      }
    }
    const cache = new Cache(new MemoryStore());
    await cache.set(["k"], new Forged());
    expect(await cache.get(["k"])).toEqual({ __cache_escaped__: 1, v: "LEAK" });
  });
});

describe("serialization guard follows what JSON would store", () => {
  it("runs toJSON once per write and stores the value that was checked", async () => {
    let calls = 0;
    class Counter {
      toJSON() {
        calls++;
        return { n: calls };
      }
    }
    const cache = new Cache(new MemoryStore());
    await cache.set(["c"], new Counter());
    expect(calls).toBe(1);
    expect(await cache.get(["c"])).toEqual({ n: 1 });
  });

  it("rejects a class whose toJSON returns undefined", async () => {
    class Shape {
      toJSON() {
        return undefined;
      }
    }
    const cache = new Cache(new MemoryStore());
    await expect(cache.set(["k"], new Shape())).rejects.toThrow(TypeError);
  });

  it("rejects a class instance holding a function field", async () => {
    class Money {
      amount = 5;
      cb = () => 1;
    }
    const cache = new Cache(new MemoryStore());
    await expect(cache.set(["k"], new Money())).rejects.toThrow(TypeError);
  });
});

describe("getOrSet lock treats a cached null as a hit", () => {
  it("a waiter that finds null under the lock does not recompute", async () => {
    const cache = new Cache(new MemoryStore());
    let computes = 0;
    const holder = cache.getOrSet(
      ["n"],
      () => {
        computes++;
        return null;
      },
      { lock: { waitFor: 2000 } },
    );
    const waiter = cache.getOrSet(
      ["n"],
      () => {
        computes++;
        return "recomputed";
      },
      { lock: { waitFor: 2000 } },
    );
    await Promise.all([holder, waiter]);
    expect(computes).toBe(1);
  });
});

describe("waitFor must be finite and non-negative", () => {
  it("withoutOverlapping rejects a NaN waitFor", async () => {
    const cache = new Cache(new MemoryStore());
    await expect(cache.withoutOverlapping(["l"], () => 1, { waitFor: Number.NaN })).rejects.toThrow(RangeError);
  });

  it("funnel rejects a negative waitFor", async () => {
    const cache = new Cache(new MemoryStore());
    await expect(cache.funnel(["f"], { limit: 1, waitFor: -1 }, () => 1)).rejects.toThrow(RangeError);
  });
});

describe("non-JSON values are rejected, not coerced", () => {
  it.each([
    ["NaN", Number.NaN],
    ["Infinity", Number.POSITIVE_INFINITY],
    ["-Infinity", Number.NEGATIVE_INFINITY],
  ])("rejects %s instead of storing it as null", async (_name, value) => {
    const cache = new Cache(new MemoryStore());
    await expect(cache.set(["n"], value)).rejects.toThrow(TypeError);
  });

  it("rejects a non-finite number nested in an object", async () => {
    const cache = new Cache(new MemoryStore());
    await expect(cache.set(["n"], { avg: Number.POSITIVE_INFINITY })).rejects.toThrow(TypeError);
  });

  it("rejects a sparse array hole instead of storing it as null", async () => {
    const cache = new Cache(new MemoryStore());
    const sparse: unknown[] = [];
    sparse[1] = 1;
    await expect(cache.set(["s"], sparse)).rejects.toThrow(TypeError);
  });

  it("rejects an invalid Date instead of storing it as null", async () => {
    const cache = new Cache(new MemoryStore());
    await expect(cache.set(["d"], new Date("not a date"))).rejects.toThrow(TypeError);
  });
});

describe("putMany validates the whole batch before writing any entry", () => {
  it("a non-finite value in a later entry leaves earlier entries unwritten", async () => {
    const cache = new Cache(new MemoryStore());
    await expect(
      cache.putMany([
        [["a"], 1],
        [["b"], Number.NaN],
      ]),
    ).rejects.toThrow(TypeError);
    expect(await cache.get(["a"])).toBeUndefined();
  });

  it("an invalid key in a later entry leaves earlier entries unwritten", async () => {
    const cache = new Cache(new MemoryStore());
    await expect(
      cache.putMany([
        [["a"], 1],
        [[undefined as never], 2],
      ]),
    ).rejects.toThrow();
    expect(await cache.get(["a"])).toBeUndefined();
  });

  it("an invalid per-value ttl in a later entry leaves earlier entries unwritten", async () => {
    const cache = new Cache(new MemoryStore());
    await expect(
      cache.putMany(
        [
          [["x"], 1],
          [["y"], 2],
        ],
        { ttl: (v: unknown) => (v === 2 ? 0 : 1000) },
      ),
    ).rejects.toThrow(RangeError);
    expect(await cache.get(["x"])).toBeUndefined();
  });
});
