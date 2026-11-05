import { describe, expect, it } from "vitest";
import { MemoryStore } from "../src/drivers/memory.ts";
import { Cache, LockTimeoutError } from "../src/index.ts";
import { makeClock } from "../src/testing/index.ts";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function counted() {
  let computes = 0;
  const compute = async () => {
    computes++;
    await sleep(50);
    return "v";
  };
  return { compute, computes: () => computes };
}

describe("getOrSet with lock (in-process)", () => {
  it("two concurrent getOrSet(lock) with a waiting lock compute once", async () => {
    const cache = new Cache(new MemoryStore());
    const { compute, computes } = counted();
    const opts = { lock: { waitFor: 2_000 } };
    const [a, b] = await Promise.all([cache.getOrSet("k", compute, opts), cache.getOrSet("k", compute, opts)]);
    expect(computes()).toBe(1);
    expect([a, b]).toEqual(["v", "v"]);
  });

  it("lock: true with the default waitFor 0 fails the concurrent loser with LockTimeoutError", async () => {
    const cache = new Cache(new MemoryStore());
    const { compute, computes } = counted();
    const results = await Promise.allSettled([
      cache.getOrSet("k", compute, { lock: true }),
      cache.getOrSet("k", compute, { lock: true }),
    ]);
    expect(computes()).toBe(1);
    const rejected = results.filter((r) => r.status === "rejected");
    expect(rejected).toHaveLength(1);
    expect((rejected[0] as PromiseRejectedResult).reason).toBeInstanceOf(LockTimeoutError);
  });

  it("without lock (default), concurrent getOrSet shares one in-flight compute", async () => {
    const cache = new Cache(new MemoryStore());
    const { compute, computes } = counted();
    await Promise.all([cache.getOrSet("k", compute), cache.getOrSet("k", compute)]);
    expect(computes()).toBe(1);
  });

  it("a hit under lock returns the stored value without computing", async () => {
    const cache = new Cache(new MemoryStore());
    await cache.set("k", "stored");
    const { compute, computes } = counted();
    expect(await cache.getOrSet("k", compute, { lock: true })).toBe("stored");
    expect(computes()).toBe(0);
  });

  it("the locked miss stores with the given ttl", async () => {
    const clock = makeClock();
    const cache = new Cache(new MemoryStore({ now: clock.now }));
    expect(await cache.getOrSet("k", () => "v", { lock: true, ttl: 1_000 })).toBe("v");
    clock.advance(1_001);
    expect(await cache.get("k")).toBeUndefined();
  });
});
