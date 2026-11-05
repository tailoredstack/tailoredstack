import { describe, expect, it } from "vitest";
import { Cache } from "../src/cache.ts";
import { MemoryStore } from "../src/drivers/memory.ts";
import { makeClock } from "../src/testing/index.ts";

// Internal marker keys, spelled out here on purpose: user values must survive even when shaped like them.
const flexShaped = () => ({ __cache_flexible__: 1, v: "user", freshUntil: 1, staleUntil: 2 });
const escapeShaped = () => ({ __cache_escaped__: 1, v: "user" });

function setup() {
  const clock = makeClock();
  return { clock, cache: new Cache(new MemoryStore({ now: clock.now })) };
}

describe("envelope collision: user values shaped like internal envelopes", () => {
  it("a flexible-shaped object round-trips through set/get", async () => {
    const { cache } = setup();
    await cache.set("k", flexShaped());
    expect(await cache.get("k")).toEqual(flexShaped());
  });

  it("a flexible-shaped object round-trips through getOrSet", async () => {
    const { cache } = setup();
    expect(await cache.getOrSet("k", flexShaped)).toEqual(flexShaped());
    expect(await cache.get("k")).toEqual(flexShaped());
  });

  it("a flexible-shaped object round-trips through many", async () => {
    const { cache } = setup();
    await cache.set("k", flexShaped());
    expect(await cache.many(["k"])).toEqual([flexShaped()]);
  });

  it("a flexible-shaped object round-trips through add", async () => {
    const { cache } = setup();
    expect(await cache.add("k", flexShaped())).toBe(true);
    expect(await cache.get("k")).toEqual(flexShaped());
  });

  it("a flexible-shaped object round-trips through putMany", async () => {
    const { cache } = setup();
    await cache.putMany([["k", flexShaped()]]);
    expect(await cache.get("k")).toEqual(flexShaped());
  });

  it("an escape-shaped object round-trips through set/get", async () => {
    const { cache } = setup();
    await cache.set("k", escapeShaped());
    expect(await cache.get("k")).toEqual(escapeShaped());
  });

  it("a flexible() value still works and is served fresh", async () => {
    const { cache } = setup();
    let calls = 0;
    const compute = () => ++calls;
    expect(await cache.flexible("k", [1000, 2000], compute)).toBe(1);
    expect(await cache.flexible("k", [1000, 2000], compute)).toBe(1);
    expect(calls).toBe(1);
  });

  it("a flexible() value shaped like an internal envelope round-trips", async () => {
    const { cache } = setup();
    expect(await cache.flexible("k", [1000, 2000], flexShaped)).toEqual(flexShaped());
    expect(await cache.flexible("k", [1000, 2000], () => "other")).toEqual(flexShaped());
  });
});
