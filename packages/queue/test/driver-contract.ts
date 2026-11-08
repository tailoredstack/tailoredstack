import { afterEach, describe, expect, it } from "vitest";
import { InvalidArgumentError, LimitError, QueueClosedError } from "../src/errors.ts";
import { createQueue } from "../src/queue.ts";
import type { Driver, DriverCapabilities, Queue } from "../src/types.ts";
import * as actors from "./actors.ts";
import {
  at,
  BAD_TIMES,
  empty,
  expectSize,
  type Harness,
  MAX_PAYLOAD_BYTES,
  mustReserve,
  NAME,
  SECOND,
  T0,
  TWELVE_HOURS,
} from "./actors.ts";

const HUNDRED_YEARS = 100 * 365 * 24 * 3_600_000;

export interface DriverFixture {
  readonly driver: Driver;
  /** A second driver on the same backend. Required when `capabilities.shared` is true. */
  readonly peer?: Driver;
}

export interface DriverContractOptions {
  readonly name: string;
  /** Capabilities the driver must declare. The suite is written against these values. */
  readonly capabilities: DriverCapabilities;
  /** Returns a fresh, empty backend for every test. */
  readonly fixture: () => Promise<DriverFixture>;
}

// Runs the full v8 contract (spec §5 cases, §6 actors) against one driver.
export function driverContract(options: DriverContractOptions): void {
  const { capabilities } = options;
  const { exactCounts, shared } = capabilities;

  describe(`queue driver contract: ${options.name}`, () => {
    const opened: Queue[] = [];

    afterEach(async () => {
      await Promise.all(opened.splice(0).map((queue) => queue.close()));
    });

    async function open(): Promise<Harness> {
      const fixture = await options.fixture();
      const queue = createQueue(fixture.driver);
      opened.push(queue);
      let peer: Queue | undefined;
      if (fixture.peer !== undefined) {
        peer = createQueue(fixture.peer);
        opened.push(peer);
      }
      return { queue, peer, exactCounts };
    }

    it("declares the capabilities the suite is written against", async () => {
      const fixture = await options.fixture();
      expect(fixture.driver.capabilities).toEqual(capabilities);
      for (const driver of [fixture.driver, fixture.peer]) {
        if (driver !== undefined) {
          await createQueue(driver).close();
        }
      }
    });

    describe("push", () => {
      it("P1: availableAt == now is reservable at once", async () => {
        const { queue } = await open();
        const id = await queue.push(NAME, "p1", at(T0));
        const reserved = await mustReserve(queue, T0, T0 + SECOND);
        expect(reserved.id).toBe(id);
        expect(reserved.payload).toBe("p1");
      });

      it("P2: availableAt > now is delayed until availableAt", async () => {
        const h = await open();
        await h.queue.push(NAME, "p2", at(T0 + SECOND));
        await expectSize(h.queue, exactCounts, { ready: 0, delayed: 1, reserved: 0 }, T0);
        expect(await h.queue.reserve(NAME, T0, T0 + SECOND)).toBeNull();
        expect(await h.queue.reserve(NAME, T0 + SECOND, T0 + 2 * SECOND)).not.toBeNull();
      });

      it("P3: availableAt < now is clamped to now, and the clamped value orders the message", async () => {
        const { queue } = await open();
        await queue.push(NAME, "b", at(T0, T0));
        await queue.push(NAME, "a", at(T0 - SECOND, T0));
        const first = await mustReserve(queue, T0, T0 + SECOND);
        expect(first.payload).toBe("b");
        const second = await mustReserve(queue, T0, T0 + SECOND);
        expect(second.payload).toBe("a");
      });

      it("P4: an empty payload is stored and returned", async () => {
        const { queue } = await open();
        await queue.push(NAME, "", at(T0));
        const reserved = await mustReserve(queue, T0, T0 + SECOND);
        expect(reserved.payload).toBe("");
      });

      it("P5: a non-string payload or a lone surrogate is InvalidArgumentError and stores nothing", async () => {
        const h = await open();
        await expect(h.queue.push(NAME, 42 as unknown as string, at(T0))).rejects.toThrow(InvalidArgumentError);
        await expect(h.queue.push(NAME, "bad\uD800", at(T0))).rejects.toThrow(InvalidArgumentError);
        await expect(h.queue.push(NAME, "bad\uDC00x", at(T0))).rejects.toThrow(InvalidArgumentError);
        await expectSize(h.queue, exactCounts, empty(), T0);
      });

      it("P6: a payload over 262144 UTF-8 bytes is LimitError and stores nothing", async () => {
        const h = await open();
        await expect(h.queue.push(NAME, "a".repeat(MAX_PAYLOAD_BYTES + 1), at(T0))).rejects.toThrow(LimitError);
        // 131072 "é" is 262144 bytes; one more ASCII byte is 262145.
        await expect(h.queue.push(NAME, `${"é".repeat(MAX_PAYLOAD_BYTES / 2)}a`, at(T0))).rejects.toThrow(LimitError);
        await expectSize(h.queue, exactCounts, empty(), T0);
      });

      it("P7: a bad name is InvalidArgumentError; an 80-character name is accepted", async () => {
        const h = await open();
        for (const name of ["", "a".repeat(81), "a/b", "a.b", "a b"]) {
          await expect(h.queue.push(name, "x", at(T0))).rejects.toThrow(InvalidArgumentError);
        }
        await expectSize(h.queue, exactCounts, empty(), T0);
        await expect(h.queue.push("a".repeat(80), "x", at(T0))).resolves.toEqual(expect.any(String));
      });

      it("P8: NaN, Infinity, negative, and fractional times are InvalidArgumentError", async () => {
        const h = await open();
        for (const bad of BAD_TIMES) {
          await expect(h.queue.push(NAME, "x", at(bad, T0))).rejects.toThrow(InvalidArgumentError);
          await expect(h.queue.push(NAME, "x", at(T0, bad))).rejects.toThrow(InvalidArgumentError);
        }
        await expectSize(h.queue, exactCounts, empty(), T0);
      });

      if (Number.isFinite(capabilities.maxPushDelayMs)) {
        it("P9: a delay over maxPushDelayMs is LimitError; exactly maxPushDelayMs is accepted", async () => {
          const h = await open();
          const max = capabilities.maxPushDelayMs;
          await expect(h.queue.push(NAME, "p9", at(T0 + max + 1))).rejects.toThrow(LimitError);
          await expectSize(h.queue, exactCounts, empty(), T0);
          await expect(h.queue.push(NAME, "p9", at(T0 + max))).resolves.toEqual(expect.any(String));
        });
      } else {
        it("P9: with no maxPushDelayMs cap, a push 100 years ahead is accepted", async () => {
          const h = await open();
          await expect(h.queue.push(NAME, "p9", at(T0 + HUNDRED_YEARS))).resolves.toEqual(expect.any(String));
        });
      }

      it("P10: the same payload pushed twice yields two messages with two ids", async () => {
        const { queue } = await open();
        const first = await queue.push(NAME, "same", at(T0));
        const second = await queue.push(NAME, "same", at(T0));
        expect(second).not.toBe(first);
        const a = await mustReserve(queue, T0, T0 + SECOND);
        const b = await mustReserve(queue, T0, T0 + SECOND);
        expect([a.id, b.id].sort()).toEqual([first, second].sort());
      });

      it("P11: ids are unique across names", async () => {
        const { queue } = await open();
        const ids: string[] = [];
        for (const name of ["alpha", "beta"]) {
          for (let i = 0; i < 3; i++) {
            ids.push(await queue.push(name, "x", at(T0)));
          }
        }
        expect(new Set(ids).size).toBe(6);
      });
    });

    describe("reserve", () => {
      it("S1: an empty queue reserves null", async () => {
        const { queue } = await open();
        expect(await queue.reserve(NAME, T0, T0 + SECOND)).toBeNull();
      });

      it("S2: a queue holding only delayed messages reserves null", async () => {
        const { queue } = await open();
        await queue.push(NAME, "s2", at(T0 + SECOND));
        expect(await queue.reserve(NAME, T0, T0 + SECOND)).toBeNull();
      });

      it("S3: one ready message reserves with attempts 1", async () => {
        const { queue } = await open();
        await queue.push(NAME, "s3", at(T0));
        const reserved = await mustReserve(queue, T0, T0 + SECOND);
        expect(reserved.payload).toBe("s3");
        expect(reserved.attempts).toBe(1);
      });

      it("S4: the earlier availableAt is reserved first", async () => {
        const { queue } = await open();
        await queue.push(NAME, "late", at(T0 + 200, T0));
        await queue.push(NAME, "early", at(T0 + 100, T0));
        const first = await mustReserve(queue, T0 + 300, T0 + 1_300);
        expect(first.payload).toBe("early");
        const second = await mustReserve(queue, T0 + 300, T0 + 1_300);
        expect(second.payload).toBe("late");
      });

      it("S5: equal availableAt is ordered by push sequence", async () => {
        const { queue } = await open();
        await queue.push(NAME, "first", at(T0));
        await queue.push(NAME, "second", at(T0));
        const reserved = await mustReserve(queue, T0, T0 + SECOND);
        expect(reserved.payload).toBe("first");
      });

      it("S6: when every ready message is leased, reserve returns null", async () => {
        const { queue } = await open();
        await queue.push(NAME, "s6", at(T0));
        await mustReserve(queue, T0, T0 + SECOND);
        expect(await queue.reserve(NAME, T0, T0 + SECOND)).toBeNull();
      });

      it("S7: leaseUntil <= now is InvalidArgumentError", async () => {
        const { queue } = await open();
        await expect(queue.reserve(NAME, T0, T0)).rejects.toThrow(InvalidArgumentError);
        await expect(queue.reserve(NAME, T0, T0 - 1)).rejects.toThrow(InvalidArgumentError);
      });

      it("S8: a lease window over 12 hours is LimitError; exactly 12 hours is accepted", async () => {
        const { queue } = await open();
        await expect(queue.reserve(NAME, T0, T0 + TWELVE_HOURS + 1)).rejects.toThrow(LimitError);
        await expect(queue.reserve(NAME, T0, T0 + TWELVE_HOURS)).resolves.toBeNull();
      });

      it("S9: a lease that ends at now == leaseUntil is ready again, with attempts+1 and a new lease", async () => {
        const { queue } = await open();
        await queue.push(NAME, "s9", at(T0));
        const first = await mustReserve(queue, T0, T0 + SECOND);
        const second = await mustReserve(queue, T0 + SECOND, T0 + 2 * SECOND);
        expect(second.id).toBe(first.id);
        expect(second.attempts).toBe(2);
        expect(await first.delete(T0 + SECOND)).toBe(false);
        expect(await second.delete(T0 + SECOND)).toBe(true);
      });

      it("S10: a released message keeps its old availableAt and sorts before a newer ready message", async () => {
        const { queue } = await open();
        await queue.push(NAME, "A", at(T0));
        const a = await mustReserve(queue, T0, T0 + SECOND);
        expect(await a.release(T0, T0)).toBe(true);
        await queue.push(NAME, "B", at(T0 + 100, T0 + 100));
        const first = await mustReserve(queue, T0 + 200, T0 + 1_200);
        expect(first.payload).toBe("A");
      });
    });

    describe("delete", () => {
      it("D1: deleting a current lease is true and the message is gone", async () => {
        const { queue } = await open();
        await queue.push(NAME, "d1", at(T0));
        const reserved = await mustReserve(queue, T0, T0 + SECOND);
        expect(await reserved.delete(T0 + SECOND / 2)).toBe(true);
        expect(await queue.reserve(NAME, T0 + SECOND, T0 + 2 * SECOND)).toBeNull();
      });

      it("D2: a second delete is false", async () => {
        const { queue } = await open();
        await queue.push(NAME, "d2", at(T0));
        const reserved = await mustReserve(queue, T0, T0 + SECOND);
        expect(await reserved.delete(T0)).toBe(true);
        expect(await reserved.delete(T0)).toBe(false);
      });

      it("D3: an expired lease that was not re-reserved deletes as false; a later reserve still gets it", async () => {
        const { queue } = await open();
        await queue.push(NAME, "d3", at(T0));
        const reserved = await mustReserve(queue, T0, T0 + SECOND);
        expect(await reserved.delete(T0 + SECOND)).toBe(false);
        const again = await mustReserve(queue, T0 + SECOND, T0 + 2 * SECOND);
        expect(again.id).toBe(reserved.id);
        expect(again.attempts).toBe(2);
      });

      it("D4: an expired lease that was re-reserved: the old handle is false, the new one is true", async () => {
        const { queue } = await open();
        await queue.push(NAME, "d4", at(T0));
        const old = await mustReserve(queue, T0, T0 + SECOND);
        const current = await mustReserve(queue, T0 + SECOND, T0 + 2 * SECOND);
        expect(await old.delete(T0 + SECOND)).toBe(false);
        expect(await current.delete(T0 + SECOND)).toBe(true);
      });

      it("D5: a handle released earlier deletes as false", async () => {
        const { queue } = await open();
        await queue.push(NAME, "d5", at(T0));
        const reserved = await mustReserve(queue, T0, T0 + SECOND);
        expect(await reserved.release(T0, T0)).toBe(true);
        expect(await reserved.delete(T0)).toBe(false);
        const again = await mustReserve(queue, T0, T0 + SECOND);
        expect(again.attempts).toBe(2);
      });

      it("D6: an earlier now still deletes while the lease is live by that now", async () => {
        const { queue } = await open();
        await queue.push(NAME, "d6", at(T0));
        const reserved = await mustReserve(queue, T0, T0 + SECOND);
        expect(await reserved.delete(T0 - SECOND / 2)).toBe(true);
      });

      it("D7: delete after close is QueueClosedError", async () => {
        const { queue } = await open();
        await queue.push(NAME, "d7", at(T0));
        const reserved = await mustReserve(queue, T0, T0 + SECOND);
        await queue.close();
        await expect(reserved.delete(T0)).rejects.toThrow(QueueClosedError);
      });
    });

    describe("release", () => {
      it("L1: availableAt > now is not reservable until availableAt", async () => {
        const { queue } = await open();
        await queue.push(NAME, "l1", at(T0));
        const reserved = await mustReserve(queue, T0, T0 + SECOND);
        expect(await reserved.release(T0 + 500, T0)).toBe(true);
        expect(await queue.reserve(NAME, T0 + 499, T0 + 1_499)).toBeNull();
        const again = await mustReserve(queue, T0 + 500, T0 + 1_500);
        expect(again.attempts).toBe(2);
      });

      it("L2: availableAt <= now is ready now (a past value is clamped to now)", async () => {
        const { queue } = await open();
        await queue.push(NAME, "l2", at(T0));
        const reserved = await mustReserve(queue, T0, T0 + SECOND);
        expect(await reserved.release(T0 - SECOND, T0)).toBe(true);
        const again = await mustReserve(queue, T0, T0 + SECOND);
        expect(again.payload).toBe("l2");
      });

      it("L3: release then reserve increments attempts", async () => {
        const { queue } = await open();
        await queue.push(NAME, "l3", at(T0));
        const reserved = await mustReserve(queue, T0, T0 + SECOND);
        expect(reserved.attempts).toBe(1);
        expect(await reserved.release(T0, T0)).toBe(true);
        const again = await mustReserve(queue, T0, T0 + SECOND);
        expect(again.attempts).toBe(2);
      });

      it("L4: a released message keeps its push sequence among equal availableAt", async () => {
        const { queue } = await open();
        await queue.push(NAME, "A", at(T0));
        await queue.push(NAME, "B", at(T0));
        const a = await mustReserve(queue, T0, T0 + SECOND);
        expect(a.payload).toBe("A");
        expect(await a.release(T0, T0)).toBe(true);
        const first = await mustReserve(queue, T0, T0 + SECOND);
        expect(first.payload).toBe("A");
      });

      it("L5: stale release is false: after re-reserve, after delete, after expiry, and a second release", async () => {
        const { queue } = await open();
        await queue.push(NAME, "l5a", at(T0));
        const old = await mustReserve(queue, T0, T0 + SECOND);
        const current = await mustReserve(queue, T0 + SECOND, T0 + 2 * SECOND);
        expect(await old.release(T0, T0 + SECOND)).toBe(false);
        await expectSize(queue, exactCounts, { ready: 0, delayed: 0, reserved: 1 }, T0 + SECOND);
        expect(await current.delete(T0 + SECOND)).toBe(true);

        await queue.push(NAME, "l5b", at(T0));
        const deleted = await mustReserve(queue, T0, T0 + SECOND);
        expect(await deleted.delete(T0)).toBe(true);
        expect(await deleted.release(T0, T0)).toBe(false);

        await queue.push(NAME, "l5c", at(T0));
        const released = await mustReserve(queue, T0, T0 + SECOND);
        expect(await released.release(T0, T0)).toBe(true);
        expect(await released.release(T0, T0)).toBe(false);
        await queue.push(NAME, "l5d", at(T0));
        const expired = await mustReserve(queue, T0, T0 + SECOND);
        expect(await expired.release(T0 + SECOND, T0 + SECOND)).toBe(false);
        await expectSize(queue, exactCounts, { ready: 2, delayed: 0, reserved: 0 }, T0 + SECOND);
      });

      it("L6: invalid release times are InvalidArgumentError", async () => {
        const { queue } = await open();
        await queue.push(NAME, "l6", at(T0));
        const reserved = await mustReserve(queue, T0, T0 + SECOND);
        await expect(reserved.release(Number.NaN, T0)).rejects.toThrow(InvalidArgumentError);
        await expect(reserved.release(T0, 1.5)).rejects.toThrow(InvalidArgumentError);
      });

      it("L7: a release delay over 12 hours is LimitError; exactly 12 hours is accepted", async () => {
        const { queue } = await open();
        await queue.push(NAME, "l7", at(T0));
        const reserved = await mustReserve(queue, T0, T0 + SECOND);
        await expect(reserved.release(T0 + TWELVE_HOURS + 1, T0)).rejects.toThrow(LimitError);
        expect(await reserved.release(T0 + TWELVE_HOURS, T0)).toBe(true);
      });

      it("L8: release after close is QueueClosedError", async () => {
        const { queue } = await open();
        await queue.push(NAME, "l8", at(T0));
        const reserved = await mustReserve(queue, T0, T0 + SECOND);
        await queue.close();
        await expect(reserved.release(T0, T0)).rejects.toThrow(QueueClosedError);
      });
    });

    describe("size", () => {
      it("Z1: an empty name reports zeros", async () => {
        const { queue } = await open();
        await expectSize(queue, exactCounts, empty(), T0);
      });

      it("Z2: mixed states report exact buckets (exact drivers) or bounded buckets (approximate drivers)", async () => {
        const { queue } = await open();
        await queue.push(NAME, "r1", at(T0));
        await queue.push(NAME, "d", at(T0 + SECOND));
        await queue.push(NAME, "r2", at(T0));
        await mustReserve(queue, T0, T0 + SECOND);
        await expectSize(queue, exactCounts, { ready: 1, delayed: 1, reserved: 1 }, T0);
      });

      it("Z3: a lease that expires by now moves the message to ready", async () => {
        const { queue } = await open();
        await queue.push(NAME, "z3", at(T0));
        await mustReserve(queue, T0, T0 + SECOND);
        await expectSize(queue, exactCounts, { ready: 0, delayed: 0, reserved: 1 }, T0);
        await expectSize(queue, exactCounts, { ready: 1, delayed: 0, reserved: 0 }, T0 + SECOND);
      });

      it("Z4: availableAt == now counts as ready, not delayed", async () => {
        const { queue } = await open();
        await queue.push(NAME, "z4", at(T0));
        await expectSize(queue, exactCounts, { ready: 1, delayed: 0, reserved: 0 }, T0);
      });

      it("Z5: messages on other names are not counted", async () => {
        const { queue } = await open();
        await queue.push("other", "z5", at(T0));
        await queue.push("other", "z5b", at(T0));
        await expectSize(queue, exactCounts, empty(), T0);
      });

      it("Z6: size after close is QueueClosedError", async () => {
        const { queue } = await open();
        await queue.close();
        await expect(queue.size(NAME, T0)).rejects.toThrow(QueueClosedError);
      });
    });

    describe("close", () => {
      it("C1: close twice is a no-op", async () => {
        const { queue } = await open();
        await queue.close();
        await expect(queue.close()).resolves.toBeUndefined();
      });

      it("C2: every call after close is QueueClosedError, checked before argument and stale rules", async () => {
        const { queue } = await open();
        await queue.close();
        await expect(queue.push(NAME, "c2", at(T0))).rejects.toThrow(QueueClosedError);
        await expect(queue.reserve(NAME, T0, T0)).rejects.toThrow(QueueClosedError);
        await expect(queue.reserve(NAME, T0, T0 + SECOND)).rejects.toThrow(QueueClosedError);
        await expect(queue.size("bad name", T0)).rejects.toThrow(QueueClosedError);
        await expect(queue.size(NAME, T0)).rejects.toThrow(QueueClosedError);
      });

      it("C3: an operation started before close completes, and close resolves after it", async () => {
        const { queue } = await open();
        await queue.push(NAME, "c3", at(T0));
        const order: string[] = [];
        const pending = queue.reserve(NAME, T0, T0 + SECOND).then((reserved) => {
          order.push("operation");
          return reserved;
        });
        const closing = queue.close().then(() => {
          order.push("close");
        });
        expect(await pending).not.toBeNull();
        await closing;
        expect(order).toEqual(["operation", "close"]);
      });

      it("C4: a Reserved from a closed queue throws QueueClosedError on delete and release", async () => {
        const { queue } = await open();
        await queue.push(NAME, "c4", at(T0));
        const reserved = await mustReserve(queue, T0, T0 + SECOND);
        await queue.close();
        await expect(reserved.delete(T0)).rejects.toThrow(QueueClosedError);
        await expect(reserved.release(T0, T0)).rejects.toThrow(QueueClosedError);
      });
    });

    describe("good, bad, and idiot actors", () => {
      it("G1: push, reserve, delete, release with backoff, and exact size", async () => {
        await actors.goodG1(await open());
      });

      it("G2: a worker loop until null handles each message once with attempts 1", async () => {
        await actors.goodG2(await open());
      });

      it("G3: reserve, lease expires, reserve again, delete with attempts 2", async () => {
        await actors.goodG3(await open());
      });

      it("B1: invalid names are InvalidArgumentError on every operation", async () => {
        await actors.badB1InvalidNames(await open());
      });

      it("B2: NaN, Infinity, negative, and fractional times are InvalidArgumentError", async () => {
        await actors.badB2InvalidTimes(await open());
      });

      it("B3: leaseUntil <= now is InvalidArgumentError", async () => {
        await actors.badB3LeaseNotAfterNow(await open());
      });

      it("B4: a 262145-byte payload is LimitError; 262144 bytes (multi-byte boundary) is stored", async () => {
        await actors.badB4PayloadLimit(await open());
      });

      it("B5: NUL, quotes, unicode, and newlines come back byte-exact", async () => {
        await actors.badB5ByteExact(await open());
      });

      it("B6: a Reserved after close throws QueueClosedError", async () => {
        await actors.badB6ReservedAfterClose(await open());
      });

      if (shared) {
        it("B7: a message pushed and observed through a peer instance is deleted through the Reserved handle", async () => {
          await actors.badB7SharedHandle(await open());
        });
      } else {
        it("B7: N/A, the driver is not shared, so there is no second instance to delete through", () => {
          expect(capabilities.shared).toBe(false);
        });
      }

      it("B8: 50 parallel reserves on 10 messages yield 10 distinct leases and 40 nulls", async () => {
        await actors.badB8ParallelReserve(await open());
      });

      it("B9: two parallel deletes on one Reserved yield exactly one true", async () => {
        await actors.badB9ParallelDelete(await open());
      });

      it("B10: a lease over 12 hours is LimitError; exactly 12 hours is accepted", async () => {
        await actors.badB10LeaseOverTwelveHours(await open());
      });

      it("I1: a consumer that never deletes gets the message back with attempts 2", async () => {
        await actors.idiotI1NeverDeletes(await open());
      });

      it("I2: delete after lease expiry is false and does not remove the message", async () => {
        await actors.idiotI2DeletesAfterExpiry(await open());
      });

      it("I3: a value meant as seconds is accepted and read as milliseconds", async () => {
        await actors.idiotI3SecondsAsMilliseconds(await open());
      });

      it("I4: an earlier now deletes while the lease is live by that clock", async () => {
        await actors.idiotI4EarlierNowToDelete(await open());
      });

      it("I5: release with a past availableAt is ready now", async () => {
        await actors.idiotI5PastAvailableAt(await open());
      });

      it("I6: a second release is false", async () => {
        await actors.idiotI6ReleasesTwice(await open());
      });

      it("I7: reuse after delete is false both times and does not throw", async () => {
        await actors.idiotI7ReuseAfterDelete(await open());
      });

      it("I8: ignored return values do not cause unhandled rejections", async () => {
        await actors.idiotI8IgnoresReturnValues(await open());
      });

      it("I9: two consumers never hold the same message while the lease is live", async () => {
        await actors.idiotI9TwoConsumers(await open());
      });

      it("I10: the same payload pushed twice is two messages", async () => {
        await actors.idiotI10SamePayloadTwice(await open());
      });

      it("I11: close during reserve lets the in-flight call complete; later calls throw", async () => {
        await actors.idiotI11CloseDuringReserve(await open());
      });
    });
  });
}
