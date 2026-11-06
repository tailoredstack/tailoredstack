import { describe, expect, it } from "vitest";
import { Queue } from "../src/index.ts";
import { FakeDriver } from "./fake-driver.fake.ts";

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

describe("option validation at push", () => {
  it.each([0, -1, 1.5, Number.NaN])("rejects tries of %s", async (tries) => {
    const { queue } = setup();
    await expect(queue.push({ name: "x", options: { tries } })).rejects.toThrow(RangeError);
  });

  it.each([0, -1, Number.NaN, Number.POSITIVE_INFINITY])("rejects timeout of %s", async (timeout) => {
    const { queue } = setup();
    await expect(queue.push({ name: "x", options: { timeout } })).rejects.toThrow(RangeError);
  });

  it("rejects a timeout that is not shorter than retryAfter", async () => {
    const { queue } = setup({ retryAfter: 10_000 });
    await expect(queue.push({ name: "x", options: { timeout: 10_000 } })).rejects.toThrow(RangeError);
    await expect(queue.push({ name: "x", options: { timeout: 10_001 } })).rejects.toThrow(RangeError);
    await expect(queue.push({ name: "x", options: { timeout: 9_999 } })).resolves.toBeUndefined();
  });

  it("rejects a negative or non-finite backoff", async () => {
    const { queue } = setup();
    await expect(queue.push({ name: "x", options: { tries: 3, backoff: -1 } })).rejects.toThrow(RangeError);
    await expect(queue.push({ name: "x", options: { tries: 3, backoff: [1_000, Number.NaN] } })).rejects.toThrow(
      RangeError,
    );
  });

  it("rejects a backoff array shorter than tries - 1", async () => {
    const { queue } = setup();
    await expect(queue.push({ name: "x", options: { tries: 4, backoff: [1_000, 2_000] } })).rejects.toThrow(
      RangeError,
    );
    await expect(queue.push({ name: "x", options: { tries: 4, backoff: [1_000, 2_000, 3_000] } })).resolves.toBeUndefined();
  });

  it("rejects a backoff array when tries is not set, since its length cannot be checked", async () => {
    const { queue } = setup();
    await expect(queue.push({ name: "x", options: { backoff: [1_000] } })).rejects.toThrow(RangeError);
  });

  it("rejects a non-finite retryUntil", async () => {
    const { queue } = setup();
    await expect(queue.push({ name: "x", options: { retryUntil: Number.NaN } })).rejects.toThrow(RangeError);
  });

  it("rejects a retryAfter of 0, which would reserve a job again in the same instant", () => {
    expect(() => setup({ retryAfter: 0 })).toThrow(RangeError);
  });
});

describe("options on the reserved job", () => {
  it("exposes tries, timeout, and retryUntil as configured", async () => {
    const { queue } = setup();
    await queue.push({ name: "x", options: { tries: 3, timeout: 5_000, retryUntil: 60_000 } });

    const job = await queue.pop();
    expect(job?.maxTries()).toBe(3);
    expect(job?.timeout()).toBe(5_000);
    expect(job?.retryUntil()).toBe(60_000);
  });

  it("failOnTimeout() defaults to false and reflects the option", async () => {
    const { queue } = setup();
    await queue.push({ name: "plain" });
    await queue.push({ name: "strict", options: { failOnTimeout: true } });

    expect((await queue.pop())?.failOnTimeout()).toBe(false);
    expect((await queue.pop())?.failOnTimeout()).toBe(true);
  });
});

describe("tries", () => {
  it("removes a job whose next reservation would exceed tries, instead of running it", async () => {
    const { queue } = setup();
    await queue.push({ name: "x", options: { tries: 2 } });

    const first = await queue.pop();
    await first?.release();
    const second = await queue.pop();
    expect(second?.attempts()).toBe(2);
    await second?.release();

    expect(await queue.pop()).toBeNull();
    expect(await queue.size()).toBe(0);
  });

  it("runs a job with no tries limit however many times it is reserved", async () => {
    const { queue } = setup();
    await queue.push({ name: "x" });

    for (let attempt = 1; attempt <= 5; attempt++) {
      const job = await queue.pop();
      expect(job?.attempts()).toBe(attempt);
      await job?.release();
    }
  });
});

describe("retryUntil", () => {
  it("runs a job reserved before its deadline", async () => {
    const { queue } = setup();
    await queue.push({ name: "x", options: { retryUntil: 5_000 } });

    expect(await queue.pop()).toMatchObject({ name: "x" });
  });

  it("removes a job reserved after its deadline, instead of running it", async () => {
    const { queue, advance } = setup();
    await queue.push({ name: "x", options: { retryUntil: 2_000 } });

    advance(1_500); // now is 2_500, past the deadline
    expect(await queue.pop()).toBeNull();
    expect(await queue.size()).toBe(0);
  });
});

describe("backoff on release", () => {
  it("applies a scalar backoff when release() is given no delay", async () => {
    const { queue, advance } = setup();
    await queue.push({ name: "x", options: { tries: 3, backoff: 3_000 } });

    await (await queue.pop())?.release();
    expect(await queue.pop()).toBeNull();

    advance(2_999);
    expect(await queue.pop()).toBeNull();

    advance(1);
    expect((await queue.pop())?.attempts()).toBe(2);
  });

  it("uses the entry for the attempt that failed when backoff is an array", async () => {
    const { queue, advance } = setup();
    await queue.push({ name: "x", options: { tries: 3, backoff: [1_000, 5_000] } });

    await (await queue.pop())?.release(); // attempt 1: wait 1_000
    advance(1_000);
    await (await queue.pop())?.release(); // attempt 2: wait 5_000

    advance(4_999);
    expect(await queue.pop()).toBeNull();

    advance(1);
    expect((await queue.pop())?.attempts()).toBe(3);
  });

  it("an explicit delay overrides the backoff", async () => {
    const { queue, advance } = setup();
    await queue.push({ name: "x", options: { tries: 3, backoff: 3_000 } });

    await (await queue.pop())?.release(500);
    advance(500);
    expect((await queue.pop())?.attempts()).toBe(2);
  });
});
