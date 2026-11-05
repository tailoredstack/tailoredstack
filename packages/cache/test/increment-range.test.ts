import { describe, expect, it } from "vitest";
import { Cache, MemoryStore, SqliteStore } from "../src/index.ts";

const stores: [string, () => Cache<MemoryStore> | Cache<SqliteStore>][] = [
  ["MemoryStore", () => new Cache(new MemoryStore())],
  ["SqliteStore", () => new Cache(new SqliteStore({ path: ":memory:" }))],
];

describe.each(stores)("%s: increment past the safe-integer range", (_name, make) => {
  it("throws RangeError instead of returning an unsafe result", async () => {
    const cache = make();
    await cache.set(["n"], Number.MAX_SAFE_INTEGER);
    await expect(cache.increment(["n"], 2)).rejects.toThrow(RangeError);
  });

  it("leaves the stored value unchanged after the rejected increment", async () => {
    const cache = make();
    await cache.set(["n"], Number.MAX_SAFE_INTEGER);
    await expect(cache.increment(["n"], 2)).rejects.toThrow(RangeError);
    expect(await cache.get(["n"])).toBe(Number.MAX_SAFE_INTEGER);
  });

  it("decrement below the safe range also throws and leaves the value", async () => {
    const cache = make();
    await cache.set(["n"], Number.MIN_SAFE_INTEGER);
    await expect(cache.decrement(["n"], 2)).rejects.toThrow(RangeError);
    expect(await cache.get(["n"])).toBe(Number.MIN_SAFE_INTEGER);
  });
});
