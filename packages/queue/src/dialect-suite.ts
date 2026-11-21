import { describe, expect, it } from "vitest";
import type { ClockSource, Dialect, DialectCapabilities } from "./dialect.ts";
import { QueueClosedError } from "./errors.ts";

export function exactCountsSkipTitle(title: string, name: string): string {
  return `${title} [skipped: ${name} declares capabilities.exactCounts=false]`;
}

export function orderedSkipTitle(title: string, name: string): string {
  return `${title} [skipped: ${name} declares capabilities.ordered=false]`;
}

function must<T>(value: T | null | undefined, what: string): T {
  if (value == null) throw new Error(`dialect suite: expected ${what} to be present`);
  return value;
}

const CALLER_BASE_MS = 1_000_000;
const SECOND_MS = 1_000;
// A backend clock is second-granular (SQS visibility and delays are whole seconds). Wait past the target second so a
// receive, delete, or visibility change never lands on the boundary itself.
const BACKEND_PAD_MS = 1_200;

interface SuiteClock {
  now(): number;
  after(ms: number): number;
  reach(at: number): Promise<number>;
}

// A caller clock uses the timestamps the suite passes in, so it can jump between readings for free. A backend clock
// owns time, so the suite rounds to whole seconds and really waits; reach() returns the timestamp to pass afterwards.
function suiteClock(source: ClockSource): SuiteClock {
  if (source === "backend") {
    // Floor, not ceil: an availableAt of `now` must already be due when the backend reads its own clock.
    const now = (): number => Math.floor(Date.now() / SECOND_MS) * SECOND_MS;
    return {
      now,
      // Keep a full second of headroom so a slow round-trip never lands on the boundary and shortens a lease to 0.
      after: (ms) => now() + SECOND_MS + Math.ceil(ms / SECOND_MS) * SECOND_MS,
      reach: async (at) => {
        const wait = at - Date.now() + BACKEND_PAD_MS;
        if (wait > 0) await new Promise((resolve) => setTimeout(resolve, wait));
        return Date.now();
      },
    };
  }
  return {
    now: () => CALLER_BASE_MS,
    after: (ms) => CALLER_BASE_MS + ms,
    reach: async (at) => at,
  };
}

export function runDialectSuite(name: string, create: () => Dialect, capabilities?: DialectCapabilities): void {
  describe(`Dialect suite: ${name}`, () => {
    const caps = capabilities ?? create().capabilities;
    const clock = suiteClock(caps.clockSource);

    const exact = (title: string, body: () => Promise<void>): void => {
      if (caps.exactCounts) {
        it(title, body);
      } else {
        it.skip(exactCountsSkipTitle(title, name), body);
      }
    };

    const ordered = (title: string, body: () => Promise<void>): void => {
      if (caps.ordered) {
        it(title, body);
      } else {
        it.skip(orderedSkipTitle(title, name), body);
      }
    };

    it("enqueue returns a non-empty id, and two enqueues return different ids", async () => {
      const d = create();
      const now = clock.now();
      const first = await d.enqueue("jobs", "a", { availableAt: now, now });
      const second = await d.enqueue("jobs", "b", { availableAt: now, now });
      expect(first).not.toBe("");
      expect(second).not.toBe("");
      expect(first).not.toBe(second);
    });

    exact("enqueue with availableAt == now is counted as ready", async () => {
      const d = create();
      await d.enqueue("jobs", "a", { availableAt: clock.now(), now: clock.now() });
      expect(await d.count("jobs", clock.now())).toEqual({ ready: 1, delayed: 0, reserved: 0 });
    });

    exact("enqueue with availableAt > now is counted as delayed, not ready", async () => {
      const d = create();
      await d.enqueue("jobs", "a", { availableAt: clock.after(1000), now: clock.now() });
      expect(await d.count("jobs", clock.now())).toEqual({ ready: 0, delayed: 1, reserved: 0 });
    });

    exact("count reports the ready and delayed buckets exactly for one name", async () => {
      const d = create();
      const now = clock.now();
      await d.enqueue("jobs", "r1", { availableAt: now, now });
      await d.enqueue("jobs", "r2", { availableAt: now, now });
      await d.enqueue("jobs", "d1", { availableAt: clock.after(4000), now });
      await d.enqueue("jobs", "d2", { availableAt: clock.after(4000), now });
      await d.enqueue("jobs", "d3", { availableAt: clock.after(4000), now });
      expect(await d.count("jobs", clock.now())).toEqual({ ready: 2, delayed: 3, reserved: 0 });
    });

    it("dequeue on an empty queue returns null", async () => {
      const d = create();
      expect(await d.dequeue("jobs", { now: clock.now(), leaseUntil: clock.after(1000) })).toBeNull();
    });

    it("dequeue hides a message until its lease expires, then redelivers it with attempts + 1", async () => {
      const d = create();
      const now = clock.now();
      const leaseUntil = clock.after(1000);
      const id = await d.enqueue("jobs", "a", { availableAt: now, now });
      const first = await d.dequeue("jobs", { now, leaseUntil });
      expect(first?.id).toBe(id);
      expect(first?.attempts).toBe(1);
      expect(await d.dequeue("jobs", { now: clock.after(500), leaseUntil: clock.after(1500) })).toBeNull();
      const at = await clock.reach(leaseUntil);
      const again = await d.dequeue("jobs", { now: at, leaseUntil: clock.after(2000) });
      expect(again?.id).toBe(id);
      expect(again?.attempts).toBe(2);
    });

    exact("count reports a leased message as reserved until its lease expires", async () => {
      const d = create();
      const leaseUntil = clock.after(1000);
      await d.enqueue("jobs", "a", { availableAt: clock.now(), now: clock.now() });
      await d.dequeue("jobs", { now: clock.now(), leaseUntil });
      expect(await d.count("jobs", clock.after(500))).toEqual({ ready: 0, delayed: 0, reserved: 1 });
      expect(await d.count("jobs", leaseUntil)).toEqual({ ready: 1, delayed: 0, reserved: 0 });
    });

    ordered("dequeue picks the earliest-due ready message, not the first pushed", async () => {
      const d = create();
      await d.enqueue("jobs", "late-due", { availableAt: clock.after(2000), now: clock.now() });
      await d.enqueue("jobs", "early-due", { availableAt: clock.after(1000), now: clock.now() });
      const at = await clock.reach(clock.after(2000));
      expect((await d.dequeue("jobs", { now: at, leaseUntil: clock.after(4000) }))?.payload).toBe("early-due");
      expect((await d.dequeue("jobs", { now: at, leaseUntil: clock.after(4000) }))?.payload).toBe("late-due");
    });

    it("two consumers never hold the same message while its lease is live", async () => {
      const d = create();
      const now = clock.now();
      const leaseUntil = clock.after(1000);
      await d.enqueue("jobs", "a", { availableAt: now, now });
      await d.enqueue("jobs", "b", { availableAt: now, now });
      const one = await d.dequeue("jobs", { now, leaseUntil });
      const two = await d.dequeue("jobs", { now, leaseUntil });
      expect(one).not.toBeNull();
      expect(two).not.toBeNull();
      expect(one?.id).not.toBe(two?.id);
      expect(await d.dequeue("jobs", { now: clock.after(500), leaseUntil: clock.after(1500) })).toBeNull();
    });

    it("dequeue returns id, payload, attempts, and a receipt that changes on every delivery", async () => {
      const d = create();
      const now = clock.now();
      const leaseUntil = clock.after(1000);
      const id = await d.enqueue("jobs", "hello", { availableAt: now, now });
      const first = await d.dequeue("jobs", { now, leaseUntil });
      expect(first).toEqual({ id, payload: "hello", attempts: 1, receipt: expect.any(String) });
      const at = await clock.reach(leaseUntil);
      const again = await d.dequeue("jobs", { now: at, leaseUntil: clock.after(2000) });
      expect(again?.payload).toBe("hello");
      expect(again?.receipt).not.toBe(first?.receipt);
    });

    it("acknowledge removes the message and returns true", async () => {
      const d = create();
      const now = clock.now();
      await d.enqueue("jobs", "a", { availableAt: now, now });
      const msg = await d.dequeue("jobs", { now, leaseUntil: clock.after(1000) });
      expect(await d.acknowledge("jobs", must(msg, "msg").receipt, clock.now())).toBe(true);
      expect(await d.dequeue("jobs", { now: clock.after(9000), leaseUntil: clock.after(10000) })).toBeNull();
    });

    it("a second acknowledge with the same receipt returns false", async () => {
      const d = create();
      const now = clock.now();
      await d.enqueue("jobs", "a", { availableAt: now, now });
      const msg = await d.dequeue("jobs", { now, leaseUntil: clock.after(1000) });
      expect(await d.acknowledge("jobs", must(msg, "msg").receipt, clock.now())).toBe(true);
      expect(await d.acknowledge("jobs", must(msg, "msg").receipt, clock.now())).toBe(false);
    });

    it("acknowledge after lease expiry returns false and does not throw", async () => {
      const d = create();
      const now = clock.now();
      const leaseUntil = clock.after(1000);
      await d.enqueue("jobs", "a", { availableAt: now, now });
      const msg = await d.dequeue("jobs", { now, leaseUntil });
      const at = await clock.reach(leaseUntil);
      await expect(d.acknowledge("jobs", must(msg, "msg").receipt, at)).resolves.toBe(false);
    });

    it("release after lease expiry returns false and does not throw", async () => {
      const d = create();
      const now = clock.now();
      const leaseUntil = clock.after(1000);
      await d.enqueue("jobs", "a", { availableAt: now, now });
      const msg = await d.dequeue("jobs", { now, leaseUntil });
      const at = await clock.reach(leaseUntil);
      await expect(
        d.release("jobs", must(msg, "msg").receipt, { availableAt: clock.after(2500), now: at }),
      ).resolves.toBe(false);
    });

    it("release with a delay hides the message until that time", async () => {
      const d = create();
      const now = clock.now();
      await d.enqueue("jobs", "a", { availableAt: now, now });
      const msg = await d.dequeue("jobs", { now, leaseUntil: clock.after(1000) });
      const availableAt = clock.after(4000);
      expect(await d.release("jobs", must(msg, "msg").receipt, { availableAt, now: clock.now() })).toBe(true);
      expect(await d.dequeue("jobs", { now: clock.after(3999), leaseUntil: clock.after(5000) })).toBeNull();
      const at = await clock.reach(availableAt);
      expect((await d.dequeue("jobs", { now: at, leaseUntil: clock.after(6000) }))?.payload).toBe("a");
    });

    it("a stale receipt from an earlier delivery returns false for acknowledge and release", async () => {
      const d = create();
      const now = clock.now();
      const leaseUntil = clock.after(1000);
      await d.enqueue("jobs", "a", { availableAt: now, now });
      const first = await d.dequeue("jobs", { now, leaseUntil });
      const at = await clock.reach(leaseUntil);
      const second = await d.dequeue("jobs", { now: at, leaseUntil: clock.after(2000) });
      expect(first?.receipt).not.toBe(second?.receipt);
      expect(await d.acknowledge("jobs", must(first, "first delivery").receipt, at)).toBe(false);
      expect(
        await d.release("jobs", must(first, "first delivery").receipt, { availableAt: clock.after(2600), now: at }),
      ).toBe(false);
      expect(await d.acknowledge("jobs", must(second, "second delivery").receipt, at)).toBe(true);
    });

    it("close is idempotent: await close(); await close() resolves", async () => {
      const d = create();
      await d.close();
      await d.close();
    });

    it("after close, enqueue, dequeue, acknowledge, release, and count each reject", async () => {
      const d = create();
      const now = clock.now();
      await d.enqueue("jobs", "a", { availableAt: now, now });
      const msg = await d.dequeue("jobs", { now, leaseUntil: clock.after(1000) });
      await d.close();
      await expect(d.enqueue("jobs", "b", { availableAt: now, now })).rejects.toBeInstanceOf(QueueClosedError);
      await expect(d.dequeue("jobs", { now, leaseUntil: clock.after(1000) })).rejects.toBeInstanceOf(QueueClosedError);
      await expect(d.acknowledge("jobs", must(msg, "delivery").receipt, clock.now())).rejects.toBeInstanceOf(
        QueueClosedError,
      );
      await expect(
        d.release("jobs", must(msg, "delivery").receipt, { availableAt: clock.after(1600), now: clock.now() }),
      ).rejects.toBeInstanceOf(QueueClosedError);
      await expect(d.count("jobs", clock.now())).rejects.toBeInstanceOf(QueueClosedError);
    });

    it("close on a fresh dialect resolves", async () => {
      await expect(create().close()).resolves.toBeUndefined();
    });

    exact("count ignores messages enqueued under another name", async () => {
      const d = create();
      const now = clock.now();
      await d.enqueue("jobs", "a", { availableAt: now, now });
      await d.enqueue("other", "b", { availableAt: now, now });
      await d.enqueue("other", "c", { availableAt: clock.after(8000), now });
      expect(await d.count("jobs", clock.now())).toEqual({ ready: 1, delayed: 0, reserved: 0 });
    });
  });
}
