import { describe, expect, it } from "vitest";
import { Cache, MemoryStore } from "../src/index.ts";

describe("invalid lock durations are rejected before the lock is taken", () => {
  it("withoutOverlapping: a rejected maxHoldFor does not leave the lock held", async () => {
    const cache = new Cache(new MemoryStore());
    await expect(cache.withoutOverlapping(["k"], () => 1, { maxHoldFor: 0, lockFor: 500 })).rejects.toThrow(RangeError);
    await expect(cache.withoutOverlapping(["k"], () => 2, { waitFor: 0 })).resolves.toBe(2);
  });

  it("funnel: a rejected maxHoldFor does not take a slot", async () => {
    const cache = new Cache(new MemoryStore());
    await expect(cache.funnel(["f"], { limit: 1, maxHoldFor: -5, lockFor: 500 }, () => 1)).rejects.toThrow(RangeError);
    await expect(cache.funnel(["f"], { limit: 1, waitFor: 0 }, () => 2)).resolves.toBe(2);
  });
});
