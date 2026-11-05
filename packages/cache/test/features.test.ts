import { describe, expect, it } from "vitest";
import { Cache, LockTimeoutError } from "../src/cache.ts";
import { MemoryStore } from "../src/drivers/memory.ts";
import { makeClock } from "../src/testing/index.ts";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// Fake clock drives TTL expiry; the Cache gets the same clock so flexible() windows agree with the store.
function setup() {
  const clock = makeClock();
  const store = new MemoryStore({ now: clock.now });
  return { clock, store, cache: new Cache(store, { now: clock.now }) };
}

// Real clock: for lock/funnel/concurrency tests that rely on actual waiting.
const realCache = () => new Cache(new MemoryStore());

describe("bulk operations", () => {
  it("many returns values in key order with undefined for misses", async () => {
    const { cache } = setup();
    await cache.set(["a"], 1);
    await cache.set(["c"], 3);
    expect(await cache.many([["a"], ["b"], ["c"]])).toEqual([1, undefined, 3]);
  });

  it("putMany writes every entry with a shared ttl", async () => {
    const { clock, cache } = setup();
    await cache.putMany(
      [
        [["a"], 1],
        [["b"], 2],
      ],
      { ttl: 1000 },
    );
    expect(await cache.get(["a"])).toBe(1);
    clock.advance(1000);
    expect(await cache.get(["a"])).toBeUndefined();
    expect(await cache.get(["b"])).toBeUndefined();
  });

  it("deleteMultiple returns how many keys were removed", async () => {
    const { cache } = setup();
    await cache.set(["a"], 1);
    await cache.set(["b"], 2);
    expect(await cache.deleteMultiple([["a"], ["b"], ["zz"]])).toBe(2);
  });

  it("set rejects undefined, since it cannot round-trip through JSON", async () => {
    const { cache } = setup();
    await expect(cache.set(["k"], undefined)).rejects.toThrow(TypeError);
  });
});

describe("touch and add", () => {
  it("touch extends a key's ttl and reports missing keys", async () => {
    const { clock, cache } = setup();
    await cache.set(["k"], "v", { ttl: 1000 });
    clock.advance(500);
    expect(await cache.touch(["k"], 1000)).toBe(true);
    clock.advance(900);
    expect(await cache.get(["k"])).toBe("v");
    expect(await cache.touch(["missing"], 1000)).toBe(false);
  });

  it("add stores only when absent", async () => {
    const { cache } = setup();
    expect(await cache.add(["k"], 1)).toBe(true);
    expect(await cache.add(["k"], 2)).toBe(false);
    expect(await cache.get(["k"])).toBe(1);
  });

  it("add with ttl is reusable after expiry", async () => {
    const { clock, cache } = setup();
    await cache.add(["k"], 1, { ttl: 1000 });
    clock.advance(1000);
    expect(await cache.add(["k"], 2)).toBe(true);
  });

  it("increment and decrement accept a step", async () => {
    const { cache } = setup();
    expect(await cache.increment(["n"], 5)).toBe(5);
    expect(await cache.decrement(["n"], 2)).toBe(3);
  });
});

describe("getOrSet with a ttl function", () => {
  it("derives ttl from the computed value", async () => {
    const { clock, cache } = setup();
    await cache.getOrSet(["k"], () => 5, { ttl: (v) => v * 1000 });
    clock.advance(4999);
    expect(await cache.get(["k"])).toBe(5);
    clock.advance(1);
    expect(await cache.get(["k"])).toBeUndefined();
  });
});

describe("typed accessors", () => {
  it("string returns a string or the fallback", async () => {
    const { cache } = setup();
    await cache.set(["s"], "hi");
    await cache.set(["n"], 1);
    expect(await cache.string(["s"])).toBe("hi");
    expect(await cache.string(["n"], "x")).toBe("x");
    expect(await cache.string(["missing"])).toBeUndefined();
  });

  it("integer rejects non-integers", async () => {
    const { cache } = setup();
    await cache.set(["i"], 2);
    await cache.set(["f"], 1.5);
    expect(await cache.integer(["i"])).toBe(2);
    expect(await cache.integer(["f"], 0)).toBe(0);
  });

  it("float accepts any finite number", async () => {
    const { cache } = setup();
    await cache.set(["f"], 1.5);
    await cache.set(["s"], "1.5");
    expect(await cache.float(["f"])).toBe(1.5);
    expect(await cache.float(["s"], 0)).toBe(0);
  });

  it("boolean accepts only booleans", async () => {
    const { cache } = setup();
    await cache.set(["b"], false);
    await cache.set(["n"], 0);
    expect(await cache.boolean(["b"])).toBe(false);
    expect(await cache.boolean(["n"], true)).toBe(true);
  });

  it("array accepts only arrays", async () => {
    const { cache } = setup();
    await cache.set(["a"], [1]);
    await cache.set(["o"], {});
    expect(await cache.array(["a"])).toEqual([1]);
    expect(await cache.array(["o"], [])).toEqual([]);
  });
});

describe("events", () => {
  it("emits hit and miss on get", async () => {
    const { cache } = setup();
    const hit: unknown[] = [];
    const miss: unknown[] = [];
    cache.on("hit", (e) => hit.push(e.key));
    cache.on("miss", (e) => miss.push(e.key));
    await cache.get(["k"]);
    await cache.set(["k"], 1);
    await cache.get(["k"]);
    expect(miss).toEqual([["k"]]);
    expect(hit).toEqual([["k"]]);
  });

  it("emits write, forget on delete, and invalidate", async () => {
    const { cache } = setup();
    const events: string[] = [];
    cache.on("write", () => events.push("write"));
    cache.on("forget", () => events.push("forget"));
    await cache.set(["a"], 1);
    await cache.delete(["a"]);
    await cache.set(["b"], 1);
    await cache.invalidate(["b"]);
    expect(events).toEqual(["write", "forget", "write", "forget"]);
  });

  it("unsubscribe stops delivery", async () => {
    const { cache } = setup();
    let count = 0;
    const off = cache.on("write", () => count++);
    await cache.set(["a"], 1);
    off();
    await cache.set(["b"], 1);
    expect(count).toBe(1);
  });

  it("listeners on a base cache also see writes from prefixed chains", async () => {
    const { cache } = setup();
    const keys: unknown[] = [];
    cache.on("write", (e) => keys.push(e.key));
    await cache.prefix(["u"]).set([1], 1);
    expect(keys).toEqual([["u", 1]]);
  });
});

describe("flexible (stale-while-revalidate)", () => {
  it("miss computes and returns the value", async () => {
    const { cache } = setup();
    let calls = 0;
    expect(await cache.flexible(["k"], [1000, 5000], () => ++calls)).toBe(1);
    expect(calls).toBe(1);
  });

  it("within the fresh window returns cached without computing", async () => {
    const { clock, cache } = setup();
    let calls = 0;
    const compute = () => ++calls;
    await cache.flexible(["k"], [1000, 5000], compute);
    clock.advance(999);
    expect(await cache.flexible(["k"], [1000, 5000], compute)).toBe(1);
    expect(calls).toBe(1);
  });

  it("in the stale window returns the stale value and refreshes in the background", async () => {
    const { clock, cache } = setup();
    let n = 0;
    const compute = () => ++n;
    expect(await cache.flexible(["k"], [1000, 5000], compute)).toBe(1);
    clock.advance(1000);
    expect(await cache.flexible(["k"], [1000, 5000], compute)).toBe(1);
    await sleep(20);
    expect(n).toBe(2);
    expect(await cache.flexible(["k"], [1000, 5000], compute)).toBe(2);
    expect(n).toBe(2);
  });

  it("after the stale window it computes synchronously", async () => {
    const { clock, cache } = setup();
    let n = 0;
    const compute = () => ++n;
    await cache.flexible(["k"], [1000, 5000], compute);
    clock.advance(6000);
    expect(await cache.flexible(["k"], [1000, 5000], compute)).toBe(2);
  });

  it("a failed background refresh keeps serving the stale value without throwing", async () => {
    const { clock, cache } = setup();
    let n = 0;
    const compute = () => {
      n++;
      if (n === 2) throw new Error("db down");
      return n;
    };
    await cache.flexible(["k"], [1000, 5000], compute);
    clock.advance(1000);
    expect(await cache.flexible(["k"], [1000, 5000], compute)).toBe(1);
    await sleep(20);
    expect(n).toBe(2);
  });

  it("concurrent misses compute once", async () => {
    const cache = realCache();
    let n = 0;
    const compute = async () => {
      n++;
      await sleep(20);
      return `v${n}`;
    };
    const results = await Promise.all(Array.from({ length: 5 }, () => cache.flexible(["k"], [1000, 1000], compute)));
    expect(n).toBe(1);
    expect(new Set(results).size).toBe(1);
  });
});

describe("withoutOverlapping (locks)", () => {
  it("runs the callback and returns its value", async () => {
    expect(await realCache().withoutOverlapping(["k"], () => 42)).toBe(42);
  });

  it("serializes concurrent callers on the same key", async () => {
    const cache = realCache();
    let active = 0;
    let max = 0;
    const task = () =>
      cache.withoutOverlapping(
        ["k"],
        async () => {
          active++;
          max = Math.max(max, active);
          await sleep(10);
          active--;
        },
        { waitFor: 5000 },
      );
    await Promise.all([task(), task(), task()]);
    expect(max).toBe(1);
  });

  it("different keys do not block each other", async () => {
    const cache = realCache();
    const slow = cache.withoutOverlapping(["a"], () => sleep(100));
    const started = Date.now();
    await cache.withoutOverlapping(["b"], () => 1, { waitFor: 0 });
    expect(Date.now() - started).toBeLessThan(90);
    await slow;
  });

  it("throws LockTimeoutError when the lock is not free within waitFor", async () => {
    const cache = realCache();
    const holder = cache.withoutOverlapping(["k"], () => sleep(200), { lockFor: 5000 });
    await sleep(10);
    await expect(cache.withoutOverlapping(["k"], () => 1, { waitFor: 30 })).rejects.toBeInstanceOf(LockTimeoutError);
    await holder;
  });

  it("releases the lock when the callback throws", async () => {
    const cache = realCache();
    await expect(
      cache.withoutOverlapping(["k"], () => {
        throw new Error("x");
      }),
    ).rejects.toThrow("x");
    expect(await cache.withoutOverlapping(["k"], () => 2, { waitFor: 0 })).toBe(2);
  });

  it("a crashed holder's lock expires after lockFor and can be taken over", async () => {
    const cache = realCache();
    void cache.withoutOverlapping(["k"], () => new Promise(() => {}), { lockFor: 50 });
    await sleep(60);
    expect(await cache.withoutOverlapping(["k"], () => "ok", { waitFor: 0 })).toBe("ok");
  });

  it("an expired holder releasing late does not free its successor's lock", async () => {
    const cache = realCache();
    const a = cache.withoutOverlapping(["k"], () => sleep(100), { lockFor: 50 });
    await sleep(60);
    const b = cache.withoutOverlapping(["k"], () => sleep(200), { lockFor: 5000, waitFor: 1000 });
    await sleep(60); // a has finished at ~100ms; its release must be a no-op on b's lock
    await expect(cache.withoutOverlapping(["k"], () => 1, { waitFor: 0 })).rejects.toBeInstanceOf(LockTimeoutError);
    await Promise.all([a, b]);
  });
});

describe("funnel (concurrency limit)", () => {
  it("never runs more than limit callbacks at once", async () => {
    const cache = realCache();
    let active = 0;
    let max = 0;
    const task = () =>
      cache.funnel(["api"], { limit: 2, waitFor: 5000 }, async () => {
        active++;
        max = Math.max(max, active);
        await sleep(20);
        active--;
      });
    await Promise.all([task(), task(), task(), task(), task()]);
    expect(max).toBe(2);
  });

  it("throws LockTimeoutError when no slot frees within waitFor", async () => {
    const cache = realCache();
    const holder = cache.funnel(["api"], { limit: 1, waitFor: 1000 }, () => sleep(200));
    await sleep(10);
    await expect(cache.funnel(["api"], { limit: 1, waitFor: 30 }, () => 1)).rejects.toBeInstanceOf(LockTimeoutError);
    await holder;
  });

  it("frees the slot when the callback throws", async () => {
    const cache = realCache();
    await expect(
      cache.funnel(["api"], { limit: 1 }, () => {
        throw new Error("x");
      }),
    ).rejects.toThrow("x");
    expect(await cache.funnel(["api"], { limit: 1, waitFor: 0 }, () => 7)).toBe(7);
  });

  it("rejects a non-positive limit", async () => {
    await expect(realCache().funnel(["api"], { limit: 0 }, () => 1)).rejects.toThrow(RangeError);
  });
});
