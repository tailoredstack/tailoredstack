import { describe, expect, it, vi } from "vitest";
import { Cache } from "../src/cache.ts";
import { MemoryStore } from "../src/drivers/memory.ts";
import { makeClock } from "../src/testing/index.ts";

function setup() {
  const clock = makeClock();
  const store = new MemoryStore({ now: clock.now });
  return { clock, store, cache: new Cache(store, { now: clock.now }) };
}

describe("increment/decrement step validation", () => {
  it.each([
    ["NaN", Number.NaN],
    ["1.5", 1.5],
    ["Infinity", Number.POSITIVE_INFINITY],
    ["unsafe integer", Number.MAX_SAFE_INTEGER + 1],
  ])("increment rejects %s with RangeError and stores nothing", async (_label, by) => {
    const { cache } = setup();
    await expect(cache.increment(["n"], by)).rejects.toThrow(RangeError);
    expect(await cache.has(["n"])).toBe(false);
  });

  it.each([
    ["NaN", Number.NaN],
    ["1.5", 1.5],
    ["Infinity", Number.POSITIVE_INFINITY],
  ])("decrement rejects %s with RangeError and stores nothing", async (_label, by) => {
    const { cache } = setup();
    await expect(cache.decrement(["n"], by)).rejects.toThrow(RangeError);
    expect(await cache.has(["n"])).toBe(false);
  });

  it("a rejected fractional increment does not poison the next increment", async () => {
    const { cache } = setup();
    await expect(cache.increment(["n"], 1.5)).rejects.toThrow(RangeError);
    expect(await cache.increment(["n"], 2)).toBe(2);
  });
});

describe("flexible window validation", () => {
  it("rejects fresh > stale with RangeError before any store access", async () => {
    const { cache, store } = setup();
    const spy = vi.spyOn(store, "get");
    const putSpy = vi.spyOn(store, "put");
    await expect(cache.flexible(["k"], [5000, 1000], () => 1)).rejects.toThrow(RangeError);
    expect(spy).not.toHaveBeenCalled();
    expect(putSpy).not.toHaveBeenCalled();
  });

  it("accepts fresh === stale", async () => {
    const { cache } = setup();
    expect(await cache.flexible(["k"], [1000, 1000], () => 7)).toBe(7);
  });
});

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

  it("a Map and a Set read back as {} (documented lossy limit)", async () => {
    const { cache } = setup();
    await cache.set(["m"], new Map([["a", 1]]));
    await cache.set(["s"], new Set([1]));
    expect(await cache.get(["m"])).toEqual({});
    expect(await cache.get(["s"])).toEqual({});
  });

  it("a class #private field is absent from the read value (documented lossy limit)", async () => {
    class Secret {
      #hidden = "x";
      visible = 1;
      reveal() {
        return this.#hidden;
      }
    }
    const { cache } = setup();
    await cache.set(["p"], new Secret());
    const read = (await cache.get(["p"])) as Record<string, unknown>;
    expect(read).toEqual({ visible: 1 });
    expect(Object.keys(read)).not.toContain("#hidden");
  });
});
