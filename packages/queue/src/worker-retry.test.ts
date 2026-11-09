import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Dialect } from "./dialect.ts";
import type { JobContext } from "./job.ts";
import { Job } from "./job.ts";
import { MemoryDialect } from "./memory-dialect.ts";
import { Queue } from "./queue.ts";
import { Worker } from "./worker.ts";

interface MailPayload {
  readonly to: string;
}

interface AppContext {
  readonly tenant: string;
}

const context: AppContext = { tenant: "acme" };

/** Fails with the same error object every run, so tests can assert identity. */
const boom = new Error("boom");
let failuresLeft = Number.POSITIVE_INFINITY;
const runs: string[] = [];

class Flaky extends Job<MailPayload, AppContext> {
  declare readonly __jobKey: "Flaky";

  async handle(_ctx: JobContext<AppContext>): Promise<void> {
    runs.push(this.payload.to);
    if (failuresLeft > 0) {
      failuresLeft -= 1;
      throw boom;
    }
  }
}

const jobs = { Flaky };

interface Harness {
  dialect: MemoryDialect;
  queue: Queue<typeof jobs>;
  clock: { now: number };
}

async function setup(): Promise<Harness> {
  const dialect = new MemoryDialect();
  const clock = { now: 0 };
  const queue = new Queue(dialect, { jobs, now: () => clock.now });
  await queue.enqueue(new Flaky({ to: "a" }));
  return { dialect, queue, clock };
}

function workerFor(h: Harness, options: { tries?: number; backoffBase?: number; backoffCap?: number } = {}) {
  return new Worker({ dialect: h.dialect, jobs, context, now: () => h.clock.now, lease: 100, ...options });
}

describe("Worker retry", () => {
  beforeEach(() => {
    runs.length = 0;
    failuresLeft = Number.POSITIVE_INFINITY;
  });

  it("defaults to 3 tries: an always-failing job runs 3 times, then is dropped", async () => {
    failuresLeft = Number.POSITIVE_INFINITY;
    const h = await setup();
    const worker = workerFor(h);

    await expect(worker.processOne()).rejects.toBe(boom);
    h.clock.now = 1_000;
    await expect(worker.processOne()).rejects.toBe(boom);
    h.clock.now = 3_000;
    await expect(worker.processOne()).rejects.toBe(boom);

    expect(runs).toHaveLength(3);
    h.clock.now = 1_000_000;
    await expect(h.dialect.count("default", h.clock.now)).resolves.toEqual({ ready: 0, delayed: 0, reserved: 0 });
  });

  it("hides a failed message until now + 1000 ms at attempt 1, then makes it ready", async () => {
    failuresLeft = 1;
    const h = await setup();
    const worker = workerFor(h);

    await expect(worker.processOne()).rejects.toBe(boom);
    h.clock.now = 999;
    await expect(h.dialect.count("default", 999)).resolves.toEqual({ ready: 0, delayed: 1, reserved: 0 });
    h.clock.now = 1_000;
    await expect(h.dialect.count("default", 1_000)).resolves.toEqual({ ready: 1, delayed: 0, reserved: 0 });
  });

  it("doubles the wait: attempt 2 fails -> 2000 ms, attempt 3 fails -> 4000 ms with tries 4", async () => {
    failuresLeft = Number.POSITIVE_INFINITY;
    const h = await setup();
    const worker = workerFor(h, { tries: 4 });

    await expect(worker.processOne()).rejects.toBe(boom); // attempt 1 at t=0, wait 1000
    h.clock.now = 1_000;
    await expect(worker.processOne()).rejects.toBe(boom); // attempt 2 at t=1000, wait 2000
    await expect(h.dialect.count("default", 2_999)).resolves.toEqual({ ready: 0, delayed: 1, reserved: 0 });
    h.clock.now = 3_000;
    await expect(h.dialect.count("default", 3_000)).resolves.toEqual({ ready: 1, delayed: 0, reserved: 0 });
    await expect(worker.processOne()).rejects.toBe(boom); // attempt 3 at t=3000, wait 4000
    await expect(h.dialect.count("default", 6_999)).resolves.toEqual({ ready: 0, delayed: 1, reserved: 0 });
    h.clock.now = 7_000;
    await expect(h.dialect.count("default", 7_000)).resolves.toEqual({ ready: 1, delayed: 0, reserved: 0 });
  });

  it("caps the wait: base 1000, cap 3000, tries 5 -> waits 1000, 2000, 3000, 3000", async () => {
    failuresLeft = Number.POSITIVE_INFINITY;
    const h = await setup();
    const worker = workerFor(h, { tries: 5, backoffBase: 1_000, backoffCap: 3_000 });

    for (let attempt = 0; attempt < 4; attempt += 1) {
      await expect(worker.processOne()).rejects.toBe(boom);
      const wait = [1_000, 2_000, 3_000, 3_000][attempt] ?? 0;
      await expect(h.dialect.count("default", h.clock.now + wait - 1)).resolves.toEqual({
        ready: 0,
        delayed: 1,
        reserved: 0,
      });
      h.clock.now += wait;
      await expect(h.dialect.count("default", h.clock.now)).resolves.toEqual({ ready: 1, delayed: 0, reserved: 0 });
    }
    await expect(worker.processOne()).rejects.toBe(boom); // attempt 5 is the last: dropped
    await expect(h.dialect.count("default", 1_000_000)).resolves.toEqual({ ready: 0, delayed: 0, reserved: 0 });
  });

  it("acknowledges and empties the queue when the retry succeeds", async () => {
    failuresLeft = 1;
    const h = await setup();
    const worker = workerFor(h);

    await expect(worker.processOne()).rejects.toBe(boom);
    h.clock.now = 1_000;
    await expect(worker.processOne()).resolves.toBe(true);

    expect(runs).toEqual(["a", "a"]);
    await expect(h.dialect.count("default", h.clock.now)).resolves.toEqual({ ready: 0, delayed: 0, reserved: 0 });
  });

  it("propagates the original handle error unchanged on each failed attempt", async () => {
    failuresLeft = Number.POSITIVE_INFINITY;
    const h = await setup();
    const worker = workerFor(h, { tries: 2 });

    const first = await worker.processOne().catch((e: unknown) => e);
    h.clock.now = 1_000;
    const second = await worker.processOne().catch((e: unknown) => e);

    expect(first).toBe(boom);
    expect(second).toBe(boom);
  });

  it("on the final attempt acknowledges and does not release", async () => {
    failuresLeft = Number.POSITIVE_INFINITY;
    const h = await setup();
    const release = vi.spyOn(h.dialect, "release");
    const acknowledge = vi.spyOn(h.dialect, "acknowledge");
    const worker = workerFor(h, { tries: 2 });

    await expect(worker.processOne()).rejects.toBe(boom);
    expect(release).toHaveBeenCalledTimes(1);
    expect(acknowledge).not.toHaveBeenCalled();

    h.clock.now = 1_000;
    await expect(worker.processOne()).rejects.toBe(boom);
    expect(release).toHaveBeenCalledTimes(1);
    expect(acknowledge).toHaveBeenCalledTimes(1);
    expect(acknowledge).toHaveBeenCalledWith("default", expect.any(String), 1_000);
  });

  it("propagates the original error when release returns false (stale receipt)", async () => {
    failuresLeft = Number.POSITIVE_INFINITY;
    const h = await setup();
    vi.spyOn(h.dialect, "release").mockResolvedValue(false);
    const worker = workerFor(h);

    await expect(worker.processOne()).rejects.toBe(boom);
  });

  it("propagates the original error when acknowledge returns false on the final attempt", async () => {
    failuresLeft = Number.POSITIVE_INFINITY;
    const h = await setup();
    vi.spyOn(h.dialect, "acknowledge").mockResolvedValue(false);
    const worker = workerFor(h, { tries: 1 });

    await expect(worker.processOne()).rejects.toBe(boom);
  });

  it("uses the dialect-reported attempts count for backoff", async () => {
    failuresLeft = Number.POSITIVE_INFINITY;
    const h = await setup();
    const dequeue = vi.spyOn(h.dialect, "dequeue");
    const release = vi.spyOn(h.dialect, "release");
    const worker = workerFor(h, { tries: 3 });

    await expect(worker.processOne()).rejects.toBe(boom);
    h.clock.now = 1_000;
    await expect(worker.processOne()).rejects.toBe(boom);

    const reported = await dequeue.mock.results[1]?.value;
    expect(reported?.attempts).toBe(2);
    expect(release).toHaveBeenLastCalledWith("default", expect.any(String), { availableAt: 1_000 + 2_000, now: 1_000 });
  });

  it("rejects invalid backoffBase, backoffCap, and tries in the constructor", () => {
    const dialect = new MemoryDialect();
    const mk = (options: { tries?: number; backoffBase?: number; backoffCap?: number }) =>
      () => new Worker({ dialect, jobs, context, ...options });

    expect(mk({ backoffBase: 0 })).toThrow(/backoffBase/);
    expect(mk({ backoffBase: -1 })).toThrow(/backoffBase/);
    expect(mk({ backoffBase: Number.NaN })).toThrow(/backoffBase/);
    expect(mk({ backoffBase: Number.POSITIVE_INFINITY })).toThrow(/backoffBase/);
    expect(mk({ backoffBase: 2_000, backoffCap: 1_000 })).toThrow(/backoffCap/);
    expect(mk({ backoffCap: Number.NaN })).toThrow(/backoffCap/);
    expect(mk({ backoffCap: Number.POSITIVE_INFINITY })).toThrow(/backoffCap/);
    expect(mk({ tries: 0 })).toThrow(/tries/);
    expect(mk({ tries: -1 })).toThrow(/tries/);
    expect(mk({ tries: 1.5 })).toThrow(/tries/);
    expect(mk({ tries: Number.NaN })).toThrow(/tries/);
    expect(mk({ tries: Number.POSITIVE_INFINITY })).toThrow(/tries/);
  });

  it("accepts tries 1 and base equal to cap", () => {
    const dialect = new MemoryDialect();
    expect(() => new Worker({ dialect, jobs, context, tries: 1, backoffBase: 500, backoffCap: 500 })).not.toThrow();
  });

  describe("compile-time checks", () => {
    it("types tries and backoff options as numbers", () => {
      // Nothing here runs: the function exists so tsc checks each @ts-expect-error line.
      const compileOnly = (dialect: Dialect) => {
        // @ts-expect-error tries must be a number, not a string
        new Worker({ dialect, jobs, context, tries: "3" });
        // @ts-expect-error backoffBase must be a number, not a string
        new Worker({ dialect, jobs, context, backoffBase: "1000" });
        // @ts-expect-error backoffCap must be a number, not a string
        new Worker({ dialect, jobs, context, backoffCap: "60000" });
      };
      expect(compileOnly).toBeTypeOf("function");
    });
  });
});
