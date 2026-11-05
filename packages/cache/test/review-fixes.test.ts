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
    expect(await cache.get(["k"])).not.toBe("LEAK");
  });
});

describe("serialization guard follows what JSON would store", () => {
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
