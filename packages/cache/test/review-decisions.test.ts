import { describe, expect, it } from "vitest";
import { Cache, MemoryStore } from "../src/index.ts";

// A store whose lock release fails. Acquire and extend still work, so the lock is taken normally.
function releaseFails(): MemoryStore {
  const store = new MemoryStore();
  store.forgetIf = async () => {
    throw new Error("store outage on release");
  };
  return store;
}

describe("a failed lock release does not change the callback outcome", () => {
  it("a callback that succeeds still resolves with its value", async () => {
    const cache = new Cache(releaseFails());
    await expect(cache.withoutOverlapping(["k"], () => "done")).resolves.toBe("done");
  });

  it("a callback that throws still rejects with its own error", async () => {
    const cache = new Cache(releaseFails());
    await expect(
      cache.withoutOverlapping(["k"], () => {
        throw new Error("business error");
      }),
    ).rejects.toThrow("business error");
  });

  it("a failed release is reported through the cache events", async () => {
    const cache = new Cache(releaseFails());
    const failures: unknown[] = [];
    cache.on("release-failed", (e) => failures.push(e));
    await cache.withoutOverlapping(["k"], () => "done");
    expect(failures).toHaveLength(1);
  });
});

describe("getOrSet single-flight respects the ttl", () => {
  it("concurrent misses with different numeric ttls each compute and keep their own ttl", async () => {
    const cache = new Cache(new MemoryStore());
    let computes = 0;
    const [a, b] = await Promise.all([
      cache.getOrSet(
        ["t"],
        () => {
          computes++;
          return "short";
        },
        { ttl: 1000 },
      ),
      cache.getOrSet(
        ["t"],
        () => {
          computes++;
          return "long";
        },
        { ttl: 60_000 },
      ),
    ]);
    expect(computes).toBe(2);
    expect(a).toBe("short");
    expect(b).toBe("long");
  });

  it("concurrent misses with the same numeric ttl still share one compute", async () => {
    const cache = new Cache(new MemoryStore());
    let computes = 0;
    const compute = async () => {
      computes++;
      await new Promise((r) => setTimeout(r, 20));
      return "v";
    };
    await Promise.all([cache.getOrSet(["s"], compute, { ttl: 1000 }), cache.getOrSet(["s"], compute, { ttl: 1000 })]);
    expect(computes).toBe(1);
  });

  it("concurrent misses with a ttl function do not share a compute", async () => {
    const cache = new Cache(new MemoryStore());
    let computes = 0;
    const compute = async () => {
      computes++;
      await new Promise((r) => setTimeout(r, 20));
      return "v";
    };
    await Promise.all([
      cache.getOrSet(["f"], compute, { ttl: () => 1000 }),
      cache.getOrSet(["f"], compute, { ttl: () => 1000 }),
    ]);
    expect(computes).toBe(2);
  });
});
