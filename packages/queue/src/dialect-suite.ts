import { describe, expect, it } from "vitest";
import type { Dialect } from "./dialect.ts";

/**
 * Title suffix for a case skipped because the dialect declares `exactCounts: false`.
 * Exported so the skip reason is unit-tested (see dialect-suite.test.ts).
 */
export function exactCountsSkipTitle(title: string, name: string): string {
  return `${title} [skipped: ${name} declares capabilities.exactCounts=false]`;
}

/**
 * Shared contract cases for `Dialect` implementations. Each case builds a fresh
 * dialect via `create`, so cases never share state. Cases that depend on exact
 * counts are skipped, with a named reason, when `capabilities.exactCounts` is false.
 *
 * Test infrastructure: excluded from coverage and from the build by exact path
 * (vitest.config.ts, tsconfig.json).
 */
export function runDialectSuite(name: string, create: () => Dialect): void {
  describe(`Dialect suite: ${name}`, () => {
    const { capabilities } = create();

    const exact = (title: string, body: () => Promise<void>): void => {
      if (capabilities.exactCounts) {
        it(title, body);
      } else {
        it.skip(exactCountsSkipTitle(title, name), body);
      }
    };

    it("enqueue returns a non-empty id, and two enqueues return different ids", async () => {
      const d = create();
      const first = await d.enqueue("jobs", "a", { availableAt: 1000, now: 1000 });
      const second = await d.enqueue("jobs", "b", { availableAt: 1000, now: 1000 });
      expect(first).not.toBe("");
      expect(second).not.toBe("");
      expect(first).not.toBe(second);
    });

    exact("enqueue with availableAt == now is counted as ready", async () => {
      const d = create();
      await d.enqueue("jobs", "a", { availableAt: 1000, now: 1000 });
      expect(await d.count("jobs", 1000)).toEqual({ ready: 1, delayed: 0, reserved: 0 });
    });

    exact("enqueue with availableAt > now is counted as delayed, not ready", async () => {
      const d = create();
      await d.enqueue("jobs", "a", { availableAt: 2000, now: 1000 });
      expect(await d.count("jobs", 1000)).toEqual({ ready: 0, delayed: 1, reserved: 0 });
    });

    exact("count reports the ready and delayed buckets exactly for one name", async () => {
      const d = create();
      await d.enqueue("jobs", "r1", { availableAt: 1000, now: 1000 });
      await d.enqueue("jobs", "r2", { availableAt: 1000, now: 1000 });
      await d.enqueue("jobs", "d1", { availableAt: 5000, now: 1000 });
      await d.enqueue("jobs", "d2", { availableAt: 5000, now: 1000 });
      await d.enqueue("jobs", "d3", { availableAt: 5000, now: 1000 });
      expect(await d.count("jobs", 1000)).toEqual({ ready: 2, delayed: 3, reserved: 0 });
    });

    it("dequeue on an empty queue returns null", async () => {
      const d = create();
      expect(await d.dequeue("jobs", { now: 1000, leaseUntil: 2000 })).toBeNull();
    });

    it("dequeue hides a message until its lease expires, then redelivers it with attempts + 1", async () => {
      const d = create();
      const id = await d.enqueue("jobs", "a", { availableAt: 1000, now: 1000 });
      const first = await d.dequeue("jobs", { now: 1000, leaseUntil: 2000 });
      expect(first?.id).toBe(id);
      expect(first?.attempts).toBe(1);
      expect(await d.dequeue("jobs", { now: 1500, leaseUntil: 2500 })).toBeNull();
      const again = await d.dequeue("jobs", { now: 2000, leaseUntil: 3000 });
      expect(again?.id).toBe(id);
      expect(again?.attempts).toBe(2);
    });

    exact("count reports a leased message as reserved until its lease expires", async () => {
      const d = create();
      await d.enqueue("jobs", "a", { availableAt: 1000, now: 1000 });
      await d.dequeue("jobs", { now: 1000, leaseUntil: 2000 });
      expect(await d.count("jobs", 1500)).toEqual({ ready: 0, delayed: 0, reserved: 1 });
      expect(await d.count("jobs", 2000)).toEqual({ ready: 1, delayed: 0, reserved: 0 });
    });

    it("dequeue picks the earliest-due ready message, not the first pushed", async () => {
      const d = create();
      await d.enqueue("jobs", "late-due", { availableAt: 2000, now: 1000 });
      await d.enqueue("jobs", "early-due", { availableAt: 1000, now: 1000 });
      expect((await d.dequeue("jobs", { now: 3000, leaseUntil: 4000 }))?.payload).toBe("early-due");
      expect((await d.dequeue("jobs", { now: 3000, leaseUntil: 4000 }))?.payload).toBe("late-due");
    });

    it("two consumers never hold the same message while its lease is live", async () => {
      const d = create();
      await d.enqueue("jobs", "a", { availableAt: 1000, now: 1000 });
      await d.enqueue("jobs", "b", { availableAt: 1000, now: 1000 });
      const one = await d.dequeue("jobs", { now: 1000, leaseUntil: 2000 });
      const two = await d.dequeue("jobs", { now: 1000, leaseUntil: 2000 });
      expect(one).not.toBeNull();
      expect(two).not.toBeNull();
      expect(one?.id).not.toBe(two?.id);
      expect(await d.dequeue("jobs", { now: 1500, leaseUntil: 2500 })).toBeNull();
    });

    it("dequeue returns id, payload, attempts, and a receipt that changes on every delivery", async () => {
      const d = create();
      const id = await d.enqueue("jobs", "hello", { availableAt: 1000, now: 1000 });
      const first = await d.dequeue("jobs", { now: 1000, leaseUntil: 2000 });
      expect(first).toEqual({ id, payload: "hello", attempts: 1, receipt: expect.any(String) });
      const again = await d.dequeue("jobs", { now: 2000, leaseUntil: 3000 });
      expect(again?.payload).toBe("hello");
      expect(again?.receipt).not.toBe(first?.receipt);
    });

    exact("count ignores messages enqueued under another name", async () => {
      const d = create();
      await d.enqueue("jobs", "a", { availableAt: 1000, now: 1000 });
      await d.enqueue("other", "b", { availableAt: 1000, now: 1000 });
      await d.enqueue("other", "c", { availableAt: 9000, now: 1000 });
      expect(await d.count("jobs", 1000)).toEqual({ ready: 1, delayed: 0, reserved: 0 });
    });
  });
}
