import { describe, expect, it } from "vitest";
import { Queue, type ReservedJob } from "../src/index.ts";
import { SyncDriver } from "../src/sync-driver.ts";
import { FakeDriver } from "./fake-driver.fake.ts";

function setup(handler: (job: ReservedJob) => unknown) {
  const queue = new Queue({ driver: new SyncDriver((job) => handler(job)) });
  return { queue };
}

describe("sync driver", () => {
  it("runs the handler inside push, before push resolves", async () => {
    const calls: string[] = [];
    const { queue } = setup((job) => {
      calls.push(job.name);
    });

    const pushed = queue.push({ name: "Charge" });
    expect(calls).toEqual([]);

    await pushed;
    expect(calls).toEqual(["Charge"]);
  });

  it("hands the handler the job name and data, as the other drivers do", async () => {
    let seen: { name: string; data: unknown; attempts: number } | undefined;
    const { queue } = setup((job) => {
      seen = { name: job.name, data: job.data, attempts: job.attempts() };
    });

    await queue.push({ name: "Charge", data: { cents: 500, at: new Date(0) } });

    expect(seen).toEqual({ name: "Charge", data: { cents: 500, at: "1970-01-01T00:00:00.000Z" }, attempts: 1 });
  });

  it("runs a job pushed with later immediately, since there is no worker to wait for", async () => {
    let ran = false;
    const { queue } = setup(() => {
      ran = true;
    });

    await queue.later(60_000, { name: "Later" });

    expect(ran).toBe(true);
  });

  it("runs each push once, in push order", async () => {
    const order: string[] = [];
    const { queue } = setup((job) => {
      order.push(job.name);
    });

    await queue.push({ name: "first" });
    await queue.push({ name: "second" });

    expect(order).toEqual(["first", "second"]);
  });

  it("propagates a rejected handler error to the caller of push", async () => {
    const boom = new Error("card declined");
    const { queue } = setup(async () => {
      throw boom;
    });

    await expect(queue.push({ name: "Charge" })).rejects.toBe(boom);
  });

  it("propagates a synchronous handler throw to the caller of push", async () => {
    const boom = new Error("sync boom");
    const { queue } = setup(() => {
      throw boom;
    });

    await expect(queue.push({ name: "Charge" })).rejects.toBe(boom);
  });

  it("does not store a failed inline job, so nothing is left to pop or count", async () => {
    const { queue } = setup(() => {
      throw new Error("boom");
    });

    await expect(queue.push({ name: "Charge" })).rejects.toThrow("boom");

    expect(await queue.size()).toBe(0);
    expect(await queue.pop()).toBeNull();
  });

  it("rejects an invalid job name before the handler runs", async () => {
    let ran = false;
    const { queue } = setup(() => {
      ran = true;
    });

    await expect(queue.push({ name: "" })).rejects.toThrow(TypeError);
    expect(ran).toBe(false);
  });

  it("encodes the payload exactly as the stored drivers do", async () => {
    // A stored driver saves the same encoded string the sync driver decodes. Compare the handler's view
    // with what a worker reserves from the stored path.
    const stored = new Queue({ driver: new FakeDriver(), now: () => 1_000 });
    await stored.push({ name: "Charge", data: { cents: 500, at: new Date(0) }, options: { tries: 3 } });
    const fromWorker = await stored.pop();

    let fromSync: ReservedJob | undefined;
    const { queue } = setup((job) => {
      fromSync = job;
    });
    await queue.push({ name: "Charge", data: { cents: 500, at: new Date(0) }, options: { tries: 3 } });

    expect(fromSync?.name).toBe(fromWorker?.name);
    expect(fromSync?.data).toEqual(fromWorker?.data);
    expect(fromSync?.maxTries()).toBe(fromWorker?.maxTries());
  });
});
