import { describe, expect, it } from "vitest";
import { Cache, MemoryStore } from "../src/index.ts";

describe("getOrSet single-flights concurrent misses in one process", () => {
  it("three concurrent misses run compute once", async () => {
    const cache = new Cache(new MemoryStore());
    let computes = 0;
    const compute = async () => {
      computes++;
      await new Promise((r) => setTimeout(r, 20));
      return "v";
    };
    const results = await Promise.all([
      cache.getOrSet(["s"], compute),
      cache.getOrSet(["s"], compute),
      cache.getOrSet(["s"], compute),
    ]);
    expect(results).toEqual(["v", "v", "v"]);
    expect(computes).toBe(1);
  });
});

describe("getOrSet single-flight does not cache or share a failed compute", () => {
  it("a failed single-flight compute is not shared with the next caller", async () => {
    const cache = new Cache(new MemoryStore());
    await expect(cache.getOrSet(["s"], () => Promise.reject(new Error("first")))).rejects.toThrow("first");
    expect(await cache.getOrSet(["s"], () => "second")).toBe("second");
  });
});
