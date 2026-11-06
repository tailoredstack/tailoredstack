import { describe, expect, it } from "vitest";
import { Queue } from "../src/index.ts";
import { FakeDriver } from "./fake-driver.fake.ts";

function setup(options: { retryAfter?: number; defaultQueue?: string } = {}) {
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

describe("push and pop", () => {
  it("pushes to the default queue and pops the job back with its name and data", async () => {
    const { queue } = setup();
    await queue.push({ name: "SendInvoice", data: { id: 7 } });

    const job = await queue.pop();
    expect(job).toMatchObject({ queue: "default", name: "SendInvoice", data: { id: 7 }, attempts: 1 });
  });

  it("pushOn routes the job to the named queue only", async () => {
    const { queue } = setup();
    await queue.pushOn("emails", { name: "Welcome" });

    expect(await queue.pop("default")).toBeNull();
    expect(await queue.pop("emails")).toMatchObject({ name: "Welcome", queue: "emails" });
  });

  it("returns null when the queue is empty", async () => {
    const { queue } = setup();
    expect(await queue.pop()).toBeNull();
  });

  it("delivers jobs in FIFO order within one queue", async () => {
    const { queue } = setup();
    await queue.push({ name: "first" });
    await queue.push({ name: "second" });

    expect((await queue.pop())?.name).toBe("first");
    expect((await queue.pop())?.name).toBe("second");
  });

  it("uses a configured defaultQueue for push and pop", async () => {
    const { queue } = setup({ defaultQueue: "high" });
    await queue.push({ name: "Urgent" });

    expect(await queue.pop("default")).toBeNull();
    expect(await queue.pop()).toMatchObject({ name: "Urgent", queue: "high" });
  });

  it("does not hand out a reserved job to a second pop", async () => {
    const { queue } = setup();
    await queue.push({ name: "Once" });

    expect(await queue.pop()).not.toBeNull();
    expect(await queue.pop()).toBeNull();
  });

  it("makes a reserved job ready again after retryAfter, with attempts incremented", async () => {
    const { queue, advance } = setup({ retryAfter: 5_000 });
    await queue.push({ name: "Flaky" });
    await queue.pop();

    advance(4_999);
    expect(await queue.pop()).toBeNull();

    advance(1);
    expect(await queue.pop()).toMatchObject({ name: "Flaky", attempts: 2 });
  });
});

describe("later", () => {
  it("does not make a job available before its delay has passed", async () => {
    const { queue, advance } = setup();
    await queue.later(10_000, { name: "Reminder" });

    expect(await queue.pop()).toBeNull();
    advance(9_999);
    expect(await queue.pop()).toBeNull();
    advance(1);
    expect(await queue.pop()).toMatchObject({ name: "Reminder" });
  });

  it("accepts a queue option", async () => {
    const { queue, advance } = setup();
    await queue.later(1_000, { name: "Digest" }, { queue: "mail" });

    advance(1_000);
    expect(await queue.pop("default")).toBeNull();
    expect(await queue.pop("mail")).toMatchObject({ name: "Digest" });
  });

  it("rejects a negative or non-finite delay instead of clipping it", async () => {
    const { queue } = setup();
    await expect(queue.later(-1, { name: "x" })).rejects.toThrow(RangeError);
    await expect(queue.later(Number.POSITIVE_INFINITY, { name: "x" })).rejects.toThrow(RangeError);
    await expect(queue.later(Number.NaN, { name: "x" })).rejects.toThrow(RangeError);
  });
});

describe("size counters", () => {
  it("counts pending, delayed, and reserved jobs separately", async () => {
    const { queue, advance } = setup();
    await queue.push({ name: "a" });
    await queue.push({ name: "b" });
    await queue.later(5_000, { name: "c" });
    await queue.pop();

    expect(await queue.pendingSize()).toBe(1);
    expect(await queue.delayedSize()).toBe(1);
    expect(await queue.reservedSize()).toBe(1);
    expect(await queue.size()).toBe(3);

    advance(5_000);
    expect(await queue.delayedSize()).toBe(0);
    expect(await queue.pendingSize()).toBe(2);
  });

  it("counts a reserved job as pending again once its retryAfter has passed", async () => {
    const { queue, advance } = setup({ retryAfter: 1_000 });
    await queue.push({ name: "a" });
    await queue.pop();

    advance(1_000);
    expect(await queue.reservedSize()).toBe(0);
    expect(await queue.pendingSize()).toBe(1);
  });

  it("scopes counts to one queue", async () => {
    const { queue } = setup();
    await queue.pushOn("a", { name: "x" });
    await queue.pushOn("b", { name: "y" });

    expect(await queue.size("a")).toBe(1);
    expect(await queue.size("b")).toBe(1);
    expect(await queue.size("c")).toBe(0);
  });
});

describe("payload validation", () => {
  it("rejects an empty job name", async () => {
    const { queue } = setup();
    await expect(queue.push({ name: "" })).rejects.toThrow(TypeError);
  });

  it("rejects data that JSON cannot represent instead of storing it", async () => {
    const { queue } = setup();
    await expect(queue.push({ name: "x", data: () => 1 })).rejects.toThrow(TypeError);
    expect(await queue.pop()).toBeNull();
  });

  it("rejects non-finite numbers in data instead of storing them as null", async () => {
    const { queue } = setup();
    await expect(queue.push({ name: "Charge", data: { amount: Number.NaN } })).rejects.toThrow(TypeError);
    await expect(queue.push({ name: "Charge", data: { cap: Number.POSITIVE_INFINITY } })).rejects.toThrow(TypeError);
    await expect(queue.push({ name: "Charge", data: [1, [Number.NEGATIVE_INFINITY]] })).rejects.toThrow(TypeError);
    expect(await queue.pop()).toBeNull();
  });

  it("rejects an invalid Date instead of storing it as null", async () => {
    const { queue } = setup();
    await expect(queue.push({ name: "Invoice", data: { due: new Date("not a date") } })).rejects.toThrow(TypeError);
    await expect(queue.push({ name: "Invoice", data: { order: { placed: new Date("x") } } })).rejects.toThrow(
      TypeError,
    );
    await expect(queue.push({ name: "Invoice", data: [new Date("x")] })).rejects.toThrow(TypeError);
    expect(await queue.pop()).toBeNull();
  });

  it("stores a valid Date as its ISO string", async () => {
    const { queue } = setup();
    await queue.push({ name: "Invoice", data: { due: new Date("2026-11-06T00:00:00Z") } });
    expect(await queue.pop()).toMatchObject({ data: { due: "2026-11-06T00:00:00.000Z" } });
  });

  it("stores missing data as null", async () => {
    const { queue } = setup();
    await queue.push({ name: "x" });
    expect(await queue.pop()).toMatchObject({ name: "x", data: null });
  });

  it("rejects an empty queue name", async () => {
    const { queue } = setup();
    await expect(queue.pushOn("", { name: "x" })).rejects.toThrow(TypeError);
  });

  it("rejects a negative retryAfter at construction", () => {
    expect(() => setup({ retryAfter: -1 })).toThrow(RangeError);
  });
});
