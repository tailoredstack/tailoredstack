import { describe, expect, it } from "vitest";
import { Queue, ReservationLostError } from "../src/index.ts";
import { FakeDriver } from "./fake-driver.fake.ts";

function setup(options: { retryAfter?: number } = {}) {
  let now = 1_000;
  const driver = new FakeDriver();
  const queue = new Queue({ driver, now: () => now, ...options });
  return {
    queue,
    advance: (ms: number) => {
      now += ms;
    },
  };
}

describe("fail() records the job in the failed store", () => {
  it("removes the job from the queue and records it with its error", async () => {
    const { queue } = setup();
    await queue.push({ name: "Charge", data: { cents: 500 } });

    const job = await queue.pop();
    await job?.fail(new Error("card declined"));

    expect(await queue.size()).toBe(0);
    expect(await queue.failed()).toEqual([
      expect.objectContaining({
        id: job?.id,
        queue: "default",
        name: "Charge",
        data: { cents: 500 },
        attempts: 1,
        error: "card declined",
        failedAt: 1_000,
      }),
    ]);
  });

  it("stores a non-Error value as its string form", async () => {
    const { queue } = setup();
    await queue.push({ name: "x" });

    await (await queue.pop())?.fail("timed out");
    expect((await queue.failed())[0]?.error).toBe("timed out");
  });

  it("records a default error text when fail() is called without an error", async () => {
    const { queue } = setup();
    await queue.push({ name: "x" });

    await (await queue.pop())?.fail();
    expect((await queue.failed())[0]?.error).toBe("failed");
  });

  it("stamps the failure with the queue clock", async () => {
    const { queue, advance } = setup();
    await queue.push({ name: "x" });

    advance(7_000);
    await (await queue.pop())?.fail(new Error("late"));
    expect((await queue.failed())[0]?.failedAt).toBe(8_000);
  });
});

describe("failed()", () => {
  it("lists failed jobs for one named queue", async () => {
    const { queue } = setup();
    await queue.pushOn("mail", { name: "Welcome" });
    await queue.pushOn("billing", { name: "Invoice" });
    await (await queue.pop("mail"))?.fail(new Error("a"));
    await (await queue.pop("billing"))?.fail(new Error("b"));

    expect((await queue.failed("mail")).map((j) => j.name)).toEqual(["Welcome"]);
  });

  it("lists failed jobs from every queue when no queue is named", async () => {
    const { queue } = setup();
    await queue.pushOn("mail", { name: "Welcome" });
    await queue.pushOn("billing", { name: "Invoice" });
    await (await queue.pop("mail"))?.fail(new Error("a"));
    await (await queue.pop("billing"))?.fail(new Error("b"));

    expect((await queue.failed()).map((j) => j.name).sort()).toEqual(["Invoice", "Welcome"]);
  });
});

describe("exhausted jobs are recorded, not dropped", () => {
  it("records a job whose next reservation exceeds tries, with the reason", async () => {
    const { queue } = setup();
    await queue.push({ name: "x", options: { tries: 1 } });
    await (await queue.pop())?.release();

    expect(await queue.pop()).toBeNull();
    expect(await queue.failed()).toEqual([
      expect.objectContaining({ name: "x", attempts: 2, error: "exhausted: tries" }),
    ]);
  });

  it("records a job reserved after retryUntil, with the reason", async () => {
    const { queue, advance } = setup();
    await queue.push({ name: "x", options: { retryUntil: 2_000 } });
    advance(1_500);

    expect(await queue.pop()).toBeNull();
    expect(await queue.failed()).toEqual([expect.objectContaining({ attempts: 1, error: "exhausted: retryUntil" })]);
  });
});

describe("a stale worker cannot record a failure", () => {
  it("rejects fail() after the lease expired and another worker reserved the job", async () => {
    const { queue, advance } = setup({ retryAfter: 1_000 });
    await queue.push({ name: "x" });

    const stale = await queue.pop();
    advance(1_000);
    const fresh = await queue.pop();

    await expect(stale?.fail(new Error("late"))).rejects.toThrow(ReservationLostError);
    expect(await queue.failed()).toEqual([]);
    expect(fresh?.hasFailed()).toBe(false);
  });
});

describe("retryFailed", () => {
  it("moves one failed job back to its queue, ready now, with attempts reset", async () => {
    const { queue } = setup();
    await queue.push({ name: "Retry me" });
    await (await queue.pop())?.fail(new Error("boom"));
    const [record] = await queue.failed();

    expect(await queue.retryFailed(record?.id ?? "")).toBe(true);
    expect(await queue.failed()).toEqual([]);

    const again = await queue.pop();
    expect(again).toMatchObject({ name: "Retry me", queue: "default" });
    expect(again?.attempts()).toBe(1);
  });

  it("returns false and changes nothing for an unknown id", async () => {
    const { queue } = setup();
    expect(await queue.retryFailed("missing")).toBe(false);
  });
});

describe("retryAllFailed", () => {
  it("retries every failed job on one queue and leaves other queues alone", async () => {
    const { queue } = setup();
    await queue.pushOn("mail", { name: "a" });
    await queue.pushOn("mail", { name: "b" });
    await queue.pushOn("billing", { name: "c" });
    await (await queue.pop("mail"))?.fail(new Error("x"));
    await (await queue.pop("mail"))?.fail(new Error("x"));
    await (await queue.pop("billing"))?.fail(new Error("x"));

    expect(await queue.retryAllFailed("mail")).toBe(2);
    expect(await queue.pendingSize("mail")).toBe(2);
    expect((await queue.failed()).map((j) => j.queue)).toEqual(["billing"]);
  });
});

describe("forgetFailed", () => {
  it("permanently deletes one failed record and returns true", async () => {
    const { queue } = setup();
    await queue.push({ name: "x" });
    await (await queue.pop())?.fail(new Error("x"));
    const [record] = await queue.failed();

    expect(await queue.forgetFailed(record?.id ?? "")).toBe(true);
    expect(await queue.failed()).toEqual([]);
    expect(await queue.retryFailed(record?.id ?? "")).toBe(false);
  });

  it("returns false for an unknown id", async () => {
    const { queue } = setup();
    expect(await queue.forgetFailed("missing")).toBe(false);
  });
});

describe("flushFailed", () => {
  it("deletes every failed record on one queue and returns the count", async () => {
    const { queue } = setup();
    await queue.pushOn("mail", { name: "a" });
    await queue.pushOn("mail", { name: "b" });
    await queue.pushOn("billing", { name: "c" });
    await (await queue.pop("mail"))?.fail(new Error("x"));
    await (await queue.pop("mail"))?.fail(new Error("x"));
    await (await queue.pop("billing"))?.fail(new Error("x"));

    expect(await queue.flushFailed("mail")).toBe(2);
    expect((await queue.failed()).map((j) => j.queue)).toEqual(["billing"]);
  });

  it("deletes failed records on every queue when no queue is named", async () => {
    const { queue } = setup();
    await queue.pushOn("mail", { name: "a" });
    await queue.pushOn("billing", { name: "b" });
    await (await queue.pop("mail"))?.fail(new Error("x"));
    await (await queue.pop("billing"))?.fail(new Error("x"));

    expect(await queue.flushFailed()).toBe(2);
    expect(await queue.failed()).toEqual([]);
  });
});

describe("pruneFailed", () => {
  it("deletes records failed more than olderThanMs ago and keeps newer ones", async () => {
    const { queue, advance } = setup();
    await queue.pushOn("mail", { name: "old" });
    await queue.pushOn("mail", { name: "new" });
    await (await queue.pop("mail"))?.fail(new Error("x")); // failed at 1_000
    advance(5_000);
    await (await queue.pop("mail"))?.fail(new Error("y")); // failed at 6_000
    advance(1_000); // now 7_000

    expect(await queue.pruneFailed(3_000)).toBe(1);
    expect((await queue.failed()).map((j) => j.name)).toEqual(["new"]);
  });

  it("rejects a negative or non-finite age", async () => {
    const { queue } = setup();
    await expect(queue.pruneFailed(-1)).rejects.toThrow(RangeError);
    await expect(queue.pruneFailed(Number.NaN)).rejects.toThrow(RangeError);
  });
});
