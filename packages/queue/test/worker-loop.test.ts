import { describe, expect, it } from "vitest";
import { Queue, runWorkerLoop, UnregisteredJobError } from "../src/index.ts";
import { FakeDriver } from "./fake-driver.fake.ts";

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** Polls a condition with small real waits. Fails the test if it does not hold within 2 s. */
async function until(condition: () => boolean): Promise<void> {
  const deadline = Date.now() + 2_000;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error("condition not met in time");
    await sleep(1);
  }
}

/** Fixed clock: a released or failed job stays unavailable, so each job runs at most once here. */
function setup(driver: FakeDriver = new FakeDriver()) {
  const queue = new Queue({ driver, now: () => 1_000 });
  return { driver, queue };
}

/** Driver that counts reserve calls, so tests can see when the loop polls. */
class CountingDriver extends FakeDriver {
  reserves = 0;
  override async reserve(...args: Parameters<FakeDriver["reserve"]>) {
    this.reserves += 1;
    return super.reserve(...args);
  }
}

describe("runWorkerLoop", () => {
  it("runs a pushed job once and stops when asked", async () => {
    const { queue } = setup();
    const ran: string[] = [];
    const controller = new AbortController();
    const q = queue.register({
      Count: async () => {
        ran.push("Count");
        controller.abort();
      },
    });
    await q.push({ name: "Count" });

    await runWorkerLoop({ queue, signal: controller.signal, pollInterval: 5, onError: () => {} });

    expect(ran).toEqual(["Count"]);
    expect(await queue.size()).toBe(0);
  });

  it("waits one poll interval between polls when the queue is empty", async () => {
    const driver = new CountingDriver();
    const { queue } = setup(driver);
    const controller = new AbortController();
    const done = runWorkerLoop({ queue, signal: controller.signal, pollInterval: 100, onError: () => {} });

    await until(() => driver.reserves === 1);
    await sleep(30);
    expect(driver.reserves).toBe(1);

    await until(() => driver.reserves >= 2);
    controller.abort();
    await done;
  });

  it("returns promptly when stopped while idle, without waiting out the interval", async () => {
    const driver = new CountingDriver();
    const { queue } = setup(driver);
    const controller = new AbortController();
    const done = runWorkerLoop({ queue, signal: controller.signal, pollInterval: 60_000, onError: () => {} });

    await until(() => driver.reserves === 1);
    const started = Date.now();
    controller.abort();
    await done;

    expect(Date.now() - started).toBeLessThan(500);
    expect(driver.reserves).toBe(1);
  });

  it("finishes the attempt in progress, then returns and reserves nothing after stop", async () => {
    const driver = new CountingDriver();
    const { queue } = setup(driver);
    const controller = new AbortController();
    let started = false;
    let release: () => void = () => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const q = queue.register({
      Slow: async () => {
        started = true;
        await gate;
      },
    });
    await q.push({ name: "Slow" });
    await q.push({ name: "Slow" });

    let returned = false;
    const done = runWorkerLoop({ queue, signal: controller.signal, pollInterval: 5, onError: () => {} }).then(() => {
      returned = true;
    });

    await until(() => started);
    controller.abort();
    await sleep(20);
    expect(returned).toBe(false);

    release();
    await done;

    expect(driver.reserves).toBe(1);
    expect(await queue.size()).toBe(1);
  });

  it("reports a failing handler through onError and keeps running", async () => {
    const { queue } = setup();
    const controller = new AbortController();
    const errors: unknown[] = [];
    const failure = new Error("boom");
    const seen: string[] = [];
    const q = queue.register({
      Bad: async () => {
        throw failure;
      },
      Good: async () => {
        seen.push("Good");
        controller.abort();
      },
    });
    await q.push({ name: "Bad", options: { tries: 1 } });
    await q.push({ name: "Good" });

    await runWorkerLoop({
      queue,
      signal: controller.signal,
      pollInterval: 5,
      onError: (error) => errors.push(error),
    });

    expect(errors).toEqual([failure]);
    expect(seen).toEqual(["Good"]);
  });

  it("reports an unregistered job through onError and keeps running", async () => {
    const { queue } = setup();
    const controller = new AbortController();
    const errors: unknown[] = [];
    const seen: string[] = [];
    const q = queue.register({
      Good: async () => {
        seen.push("Good");
        controller.abort();
      },
    });
    await queue.push({ name: "Missing" });
    await q.push({ name: "Good" });

    await runWorkerLoop({
      queue,
      signal: controller.signal,
      pollInterval: 5,
      onError: (error) => errors.push(error),
    });

    expect(errors).toHaveLength(1);
    expect(errors[0]).toBeInstanceOf(UnregisteredJobError);
    expect(seen).toEqual(["Good"]);
  });

  it("does not run a job reserved while stop was requested, and does not lose it", async () => {
    // The driver reserves the job, but its reserve call resolves only after stop is requested.
    class SlowReserveDriver extends CountingDriver {
      resume: (() => void) | undefined;
      override async reserve(...args: Parameters<FakeDriver["reserve"]>) {
        const raw = await super.reserve(...args);
        await new Promise<void>((resolve) => {
          this.resume = resolve;
        });
        return raw;
      }
    }
    const driver = new SlowReserveDriver();
    const { queue } = setup(driver);
    const controller = new AbortController();
    const ran: string[] = [];
    const q = queue.register({
      Once: async () => {
        ran.push("Once");
      },
    });
    await q.push({ name: "Once" });

    const done = runWorkerLoop({ queue, signal: controller.signal, pollInterval: 5, onError: () => {} });
    await until(() => driver.resume !== undefined);
    controller.abort();
    driver.resume?.();
    await done;

    expect(ran).toEqual([]);
    expect(await queue.size()).toBe(1);
  });

  it("reports a driver pop error and waits an interval before polling again", async () => {
    class FailingDriver extends CountingDriver {
      override async reserve(...args: Parameters<FakeDriver["reserve"]>): Promise<never> {
        await super.reserve(...args);
        throw new Error("driver down");
      }
    }
    const driver = new FailingDriver();
    const { queue } = setup(driver);
    const controller = new AbortController();
    const errors: unknown[] = [];
    const done = runWorkerLoop({
      queue,
      signal: controller.signal,
      pollInterval: 50,
      onError: (error) => errors.push(error),
    });

    await until(() => errors.length === 1);
    const before = driver.reserves;
    await sleep(100);
    controller.abort();
    await done;

    // One poll per 50 ms interval: about 2 in 100 ms. A hot loop would make hundreds.
    expect(driver.reserves - before).toBeLessThanOrEqual(4);
    expect(errors.every((e) => e instanceof Error && e.message === "driver down")).toBe(true);
  });
});
