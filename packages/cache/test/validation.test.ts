import { describe, expect, it } from "vitest";
import { Cache } from "../src/cache.ts";
import { MemoryStore } from "../src/drivers/memory.ts";
import { makeClock } from "../src/testing/index.ts";

function setup() {
  const clock = makeClock();
  const store = new MemoryStore({ now: clock.now });
  return { clock, store, cache: new Cache(store, { now: clock.now }) };
}

describe("value serialization guard", () => {
  it.each([
    ["undefined", undefined],
    ["a function", () => 1],
    ["a symbol", Symbol("s")],
    ["a bigint", 1n],
  ])("set rejects %s at the top level", async (_label, value) => {
    const { cache } = setup();
    await expect(cache.set(["k"], value)).rejects.toThrow(TypeError);
  });

  it.each([
    ["a function", () => 1],
    ["a symbol", Symbol("s")],
    ["a bigint", 1n],
    ["undefined", undefined],
  ])("set rejects %s nested in a plain object", async (_label, bad) => {
    const { cache } = setup();
    await expect(cache.set(["k"], { a: { b: bad } })).rejects.toThrow(TypeError);
  });

  it("set rejects a function nested in an array", async () => {
    const { cache } = setup();
    await expect(cache.set(["k"], [1, [() => 1]])).rejects.toThrow(TypeError);
  });

  it("a rejected set stores nothing", async () => {
    const { cache } = setup();
    await expect(cache.set(["k"], () => 1)).rejects.toThrow(TypeError);
    expect(await cache.get(["k"])).toBeUndefined();
  });

  it("does not corrupt the store: get, clear and invalidate still work after a rejected set", async () => {
    const { cache } = setup();
    await cache.set(["ok"], 1);
    await expect(cache.set(["bad"], () => 1)).rejects.toThrow(TypeError);
    expect(await cache.get(["ok"])).toBe(1);
    expect(await cache.invalidate(["ok"])).toBe(1);
    expect(await cache.clear()).toBe(0);
  });

  it("add rejects an unserializable value and stores nothing", async () => {
    const { cache } = setup();
    await expect(cache.add(["k"], () => 1)).rejects.toThrow(TypeError);
    expect(await cache.has(["k"])).toBe(false);
  });

  it("putMany rejects the whole batch when one entry is unserializable, and writes nothing", async () => {
    const { cache } = setup();
    await expect(
      cache.putMany([
        [["a"], 1],
        [["b"], () => 2],
      ]),
    ).rejects.toThrow(TypeError);
    expect(await cache.has(["a"])).toBe(false);
  });

  it("getOrSet rejects a compute result that cannot be stored", async () => {
    const { cache } = setup();
    await expect(cache.getOrSet(["k"], () => () => 1)).rejects.toThrow(TypeError);
    expect(await cache.has(["k"])).toBe(false);
  });

  it("flexible rejects a compute result that cannot be stored", async () => {
    const { cache } = setup();
    await expect(cache.flexible(["k"], [1000, 2000], () => () => 1)).rejects.toThrow(TypeError);
    expect(await cache.has(["k"])).toBe(false);
  });

  it("accepts a successful nested plain object with arrays, null, booleans and numbers", async () => {
    const { cache } = setup();
    const value = { id: 1, ok: true, none: null, tags: ["a", { deep: [1, 2, { x: "y" }] }] };
    await cache.set(["k"], value);
    expect(await cache.get(["k"])).toEqual(value);
  });

  it("still accepts Date, Map and Set (documented lossy limits, not rejected)", async () => {
    const { cache } = setup();
    await expect(cache.set(["d"], new Date(0))).resolves.toBeUndefined();
    await expect(cache.set(["m"], new Map([["a", 1]]))).resolves.toBeUndefined();
    await expect(cache.set(["s"], new Set([1]))).resolves.toBeUndefined();
  });
});
