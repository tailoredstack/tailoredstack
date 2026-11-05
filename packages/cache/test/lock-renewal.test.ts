import { describe, expect, it } from "vitest";
import { Cache, LockTimeoutError } from "../src/cache.ts";
import { MemoryStore } from "../src/drivers/memory.ts";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// Records every extendIf call and its result. `loseAfter` deletes the key before that call, simulating a takeover.
class CountingStore extends MemoryStore {
  readonly extendCalls: { key: string; ok: boolean }[] = [];
  loseAfter = Number.POSITIVE_INFINITY;

  override async extendIf(key: string, expected: unknown, ttlMs: number): Promise<boolean> {
    if (this.extendCalls.length + 1 === this.loseAfter) await this.forget(key);
    const ok = await super.extendIf(key, expected, ttlMs);
    this.extendCalls.push({ key, ok });
    return ok;
  }
}

describe("lock renewal (in-process)", () => {
  it("a callback running 3x lockFor keeps exclusivity", async () => {
    const cache = new Cache(new CountingStore());
    const first = cache.withoutOverlapping(["job"], () => sleep(450), { lockFor: 150, waitFor: 0 });
    await sleep(300);
    await expect(cache.withoutOverlapping(["job"], () => "second", { waitFor: 0 })).rejects.toBeInstanceOf(
      LockTimeoutError,
    );
    await first;
  });

  it("a callback that outlives maxHoldFor loses exclusivity", async () => {
    const cache = new Cache(new CountingStore());
    const order: string[] = [];
    const first = cache.withoutOverlapping(
      ["job"],
      async () => {
        order.push("A-enter");
        await sleep(1_500);
        order.push("A-exit");
      },
      { lockFor: 100, maxHoldFor: 300, waitFor: 0 },
    );
    await sleep(100);
    await cache.withoutOverlapping(
      ["job"],
      () => {
        order.push("B");
      },
      { lockFor: 100, waitFor: 2_000 },
    );
    await first;
    expect(order).toEqual(["A-enter", "B", "A-exit"]);
  });

  it("a late release cannot free the successor's lock", async () => {
    const store = new CountingStore();
    const cache = new Cache(store);
    let releaseB!: () => void;
    const gateB = new Promise<void>((r) => {
      releaseB = r;
    });
    // A holds past maxHoldFor (200) and lockFor (100): renewal stops, so the lock is gone by about 300 ms.
    const first = cache.withoutOverlapping(["job"], () => sleep(900), {
      lockFor: 100,
      maxHoldFor: 200,
      waitFor: 0,
    });
    await sleep(400);
    const second = cache.withoutOverlapping(["job"], () => gateB, { lockFor: 5_000, waitFor: 0 });
    await sleep(50);
    await expect(cache.withoutOverlapping(["job"], () => "third", { waitFor: 0 })).rejects.toBeInstanceOf(
      LockTimeoutError,
    );
    await first; // A's late release runs now, while B still holds: it must not free B's lock
    await expect(cache.withoutOverlapping(["job"], () => "fourth", { waitFor: 0 })).rejects.toBeInstanceOf(
      LockTimeoutError,
    );
    releaseB();
    await second;
  });

  it("renewal stops after the lock is lost", async () => {
    const store = new CountingStore();
    store.loseAfter = 2;
    const cache = new Cache(store);
    await cache.withoutOverlapping(["job"], () => sleep(600), { lockFor: 90, waitFor: 0 });
    const falses = store.extendCalls.filter((c) => !c.ok).length;
    expect(falses).toBe(1);
    expect(store.extendCalls.length).toBe(2);
  });

  it("timers are cleared when the callback throws", async () => {
    const store = new CountingStore();
    const cache = new Cache(store);
    await expect(
      cache.withoutOverlapping(
        ["job"],
        async () => {
          await sleep(200);
          throw new Error("boom");
        },
        { lockFor: 90, waitFor: 0 },
      ),
    ).rejects.toThrow("boom");
    const atThrow = store.extendCalls.length;
    expect(atThrow).toBeGreaterThan(0);
    await sleep(400);
    expect(store.extendCalls.length).toBe(atThrow);
  });

  it("funnel slots are renewed while the callback runs", async () => {
    const store = new CountingStore();
    const cache = new Cache(store);
    const first = cache.funnel(["api"], { limit: 1, lockFor: 150, waitFor: 0 }, () => sleep(450));
    await sleep(300);
    await expect(cache.funnel(["api"], { limit: 1, waitFor: 0 }, () => "second")).rejects.toBeInstanceOf(
      LockTimeoutError,
    );
    await first;
    expect(store.extendCalls.some((c) => c.ok)).toBe(true);
  });
});
