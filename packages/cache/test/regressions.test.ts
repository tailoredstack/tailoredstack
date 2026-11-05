import { describe, expect, it } from "vitest";
import { Cache, MemoryStore } from "../src/index.ts";
import { makeClock } from "../src/testing/index.ts";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe("regression: flexible stale path shares one refresh", () => {
  it("concurrent stale hits trigger a single background compute", async () => {
    const clock = makeClock();
    const cache = new Cache(new MemoryStore({ now: clock.now }), { now: clock.now });
    let computes = 0;
    const compute = async () => {
      computes++;
      await sleep(20);
      return computes;
    };
    await cache.flexible(["k"], [1000, 5000], compute);
    clock.advance(1000); // past the fresh window, inside the stale window
    await Promise.all(Array.from({ length: 5 }, () => cache.flexible(["k"], [1000, 5000], compute)));
    await sleep(60); // let the background refresh finish
    expect(computes).toBe(2);
  });
});

describe("regression: fractional ttl", () => {
  it("memory store accepts a fractional ttl", async () => {
    const cache = new Cache(new MemoryStore());
    await cache.set(["k"], "v", { ttl: 1500.5 });
    expect(await cache.get(["k"])).toBe("v");
  });
});
