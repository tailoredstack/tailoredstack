import { describe, expect, it } from "vitest";
import { Queue } from "../src/index.ts";
import { FakeDriver } from "./fake-driver.fake.ts";

const UUID_V4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

function setup(options: { retryAfter?: number } = {}) {
  let now = 1_000;
  const driver = new FakeDriver();
  const queue = new Queue({ driver, now: () => now, ...options });
  return {
    queue,
    driver,
    advance: (ms: number) => {
      now += ms;
    },
  };
}

describe("reserved job identity", () => {
  it("exposes id, queue, name, and data of the reserved job", async () => {
    const { queue } = setup();
    await queue.pushOn("emails", { name: "Welcome", data: { to: "a@example.com" } });

    const job = await queue.pop("emails");
    expect(job?.id).toEqual(expect.any(String));
    expect(job).toMatchObject({ queue: "emails", name: "Welcome", data: { to: "a@example.com" } });
  });

  it("gives each job its own v4 uuid", async () => {
    const { queue } = setup();
    await queue.push({ name: "a" });
    await queue.push({ name: "b" });

    const first = await queue.pop();
    const second = await queue.pop();
    expect(first?.uuid).toMatch(UUID_V4);
    expect(second?.uuid).toMatch(UUID_V4);
    expect(first?.uuid).not.toBe(second?.uuid);
  });

  it("keeps the same id and uuid when the job is reserved again", async () => {
    const { queue, advance } = setup({ retryAfter: 1_000 });
    await queue.push({ name: "Flaky" });
    const first = await queue.pop();

    advance(1_000);
    const second = await queue.pop();
    expect(first?.uuid).toMatch(UUID_V4);
    expect(second?.id).toBe(first?.id);
    expect(second?.uuid).toBe(first?.uuid);
  });

  it("payload() returns the stored uuid, name, and data", async () => {
    const { queue } = setup();
    await queue.push({ name: "Charge", data: { cents: 500 } });

    const job = await queue.pop();
    expect(job?.payload()).toEqual({ uuid: job?.uuid, name: "Charge", data: { cents: 500 } });
  });

  it("attempts() is 1 on the first reservation and counts each later reservation", async () => {
    const { queue, advance } = setup({ retryAfter: 1_000 });
    await queue.push({ name: "Flaky" });

    const first = await queue.pop();
    expect(first?.attempts()).toBe(1);

    advance(1_000);
    expect((await queue.pop())?.attempts()).toBe(2);
  });

  it("reports no terminal state while the job is reserved", async () => {
    const { queue } = setup();
    await queue.push({ name: "Open" });

    const job = await queue.pop();
    expect(job?.isDeleted()).toBe(false);
    expect(job?.isReleased()).toBe(false);
    expect(job?.hasFailed()).toBe(false);
  });

  it("maxTries(), timeout(), and retryUntil() return null until job options exist", async () => {
    const { queue } = setup();
    await queue.push({ name: "Plain" });

    const job = await queue.pop();
    expect(job?.maxTries()).toBeNull();
    expect(job?.timeout()).toBeNull();
    expect(job?.retryUntil()).toBeNull();
  });
});

describe("delete()", () => {
  it("removes the job so it is never reserved again", async () => {
    const { queue, advance } = setup({ retryAfter: 1_000 });
    await queue.push({ name: "Done" });

    const job = await queue.pop();
    await job?.delete();
    expect(job?.isDeleted()).toBe(true);

    advance(10 * 60_000);
    expect(await queue.pop()).toBeNull();
    expect(await queue.size()).toBe(0);
  });

  it("rejects a second delete on the same job", async () => {
    const { queue } = setup();
    await queue.push({ name: "Once" });

    const job = await queue.pop();
    await job?.delete();
    await expect(job?.delete()).rejects.toThrow();
  });
});

describe("release()", () => {
  it("hides the job for the delay, then makes it ready again with attempts incremented", async () => {
    const { queue, advance } = setup();
    await queue.push({ name: "Later" });

    const job = await queue.pop();
    await job?.release(5_000);
    expect(job?.isReleased()).toBe(true);
    expect(await queue.pop()).toBeNull();

    advance(4_999);
    expect(await queue.pop()).toBeNull();

    advance(1);
    const again = await queue.pop();
    expect(again).toMatchObject({ name: "Later" });
    expect(again?.attempts()).toBe(2);
  });

  it("with no delay makes the job ready immediately", async () => {
    const { queue } = setup();
    await queue.push({ name: "Now" });

    const job = await queue.pop();
    await job?.release();
    expect((await queue.pop())?.name).toBe("Now");
  });

  it("rejects a negative or non-finite delay", async () => {
    const { queue } = setup();
    await queue.push({ name: "x" });

    const job = await queue.pop();
    await expect(job?.release(-1)).rejects.toThrow(RangeError);
    await expect(job?.release(Number.NaN)).rejects.toThrow(RangeError);
    expect(job?.isReleased()).toBe(false);
  });
});

describe("fail()", () => {
  it("removes the job from the queue and marks it failed", async () => {
    const { queue, advance } = setup();
    await queue.push({ name: "Broken" });

    const job = await queue.pop();
    await job?.fail(new Error("boom"));
    expect(job?.hasFailed()).toBe(true);

    advance(10 * 60_000);
    expect(await queue.pop()).toBeNull();
    expect(await queue.size()).toBe(0);
  });

  it("accepts a non-Error value without throwing", async () => {
    const { queue } = setup();
    await queue.push({ name: "Odd" });

    const job = await queue.pop();
    await expect(job?.fail("timed out")).resolves.toBeUndefined();
    expect(job?.hasFailed()).toBe(true);
  });
});

describe("stale reservations", () => {
  it("a worker whose reservation expired cannot touch the job a newer worker reserved", async () => {
    const { queue, advance } = setup({ retryAfter: 1_000 });
    await queue.push({ name: "Shared" });

    const stale = await queue.pop();
    advance(1_000);
    const fresh = await queue.pop();
    expect(fresh?.attempts()).toBe(2);

    await expect(stale?.delete()).rejects.toThrow();
    await expect(stale?.release()).rejects.toThrow();
    await expect(stale?.fail(new Error("late"))).rejects.toThrow();
    expect(stale?.isDeleted()).toBe(false);
    expect(stale?.hasFailed()).toBe(false);

    await fresh?.delete();
    expect(await queue.size()).toBe(0);
  });

  it("rejects release() and fail() after the job has been deleted", async () => {
    const { queue } = setup();
    await queue.push({ name: "Gone" });

    const job = await queue.pop();
    await job?.delete();
    await expect(job?.release()).rejects.toThrow();
    await expect(job?.fail(new Error("x"))).rejects.toThrow();
  });
});
