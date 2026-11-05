import { describe, expect, it } from "vitest";
import { Cache, MemoryStore } from "../src/index.ts";

// Concurrent misses may share one compute only when their effective ttl is the same. The effective ttl is the
// per-call ttl if given, otherwise the scope ttl from ttl(ms). Before the fix, the single-flight key used only the
// per-call ttl, so a scoped and an unscoped caller on the same key shared one compute and each received the other's
// value. A gate holds the first compute open until both calls have started, so the overlap is deterministic.

function gated() {
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { gate, release };
}

describe("getOrSet single-flight respects the effective ttl, including the scope ttl", () => {
  it("scoped caller first: each caller receives its own compute result", async () => {
    const base = new Cache(new MemoryStore());
    const scoped = base.ttl(1000);
    const { gate, release } = gated();
    let computes = 0;

    const first = scoped.getOrSet(["x"], async () => {
      computes++;
      await gate;
      return "scoped";
    });
    const second = base.getOrSet(["x"], async () => {
      computes++;
      await gate;
      return "plain";
    });
    release();

    expect(await first).toBe("scoped");
    expect(await second).toBe("plain");
    expect(computes).toBe(2);
  });

  it("plain caller first: each caller receives its own compute result", async () => {
    const base = new Cache(new MemoryStore());
    const scoped = base.ttl(1000);
    const { gate, release } = gated();
    let computes = 0;

    const first = base.getOrSet(["y"], async () => {
      computes++;
      await gate;
      return "plain";
    });
    const second = scoped.getOrSet(["y"], async () => {
      computes++;
      await gate;
      return "scoped";
    });
    release();

    expect(await first).toBe("plain");
    expect(await second).toBe("scoped");
    expect(computes).toBe(2);
  });

  it("a scope ttl and an equal per-call ttl are the same effective ttl and still share one compute", async () => {
    const base = new Cache(new MemoryStore());
    const { gate, release } = gated();
    let computes = 0;

    const first = base.ttl(1000).getOrSet(["z"], async () => {
      computes++;
      await gate;
      return "v";
    });
    const second = base.getOrSet(
      ["z"],
      async () => {
        computes++;
        await gate;
        return "v";
      },
      { ttl: 1000 },
    );
    release();

    expect(await first).toBe("v");
    expect(await second).toBe("v");
    expect(computes).toBe(1);
  });
});
