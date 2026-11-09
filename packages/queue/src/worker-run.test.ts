import { beforeEach, describe, expect, it, vi } from "vitest";
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

const boom = new Error("boom");
const log: string[] = [];
let failing = new Set<string>();
let onRun: (to: string) => void = () => {};
let onSleep: (ms: number) => Promise<void> = () => Promise.resolve();

class Mail extends Job<MailPayload, AppContext> {
  declare readonly __jobKey: "Mail";

  async handle(_ctx: JobContext<AppContext>): Promise<void> {
    log.push(`run:${this.payload.to}`);
    onRun(this.payload.to);
    if (failing.has(this.payload.to)) throw boom;
  }
}

const jobs = { Mail };
const context: AppContext = { tenant: "acme" };

function setup(options: { pollInterval?: number } = {}) {
  const dialect = new MemoryDialect();
  const clock = { now: 1_000 };
  const queue = new Queue(dialect, { jobs, now: () => clock.now });
  const sleeps: number[] = [];
  const worker = new Worker({
    dialect,
    jobs,
    context,
    now: () => clock.now,
    sleep: (ms) => {
      sleeps.push(ms);
      log.push("sleep");
      return onSleep(ms);
    },
    ...options,
  });
  return { dialect, queue, worker, sleeps, clock };
}

type Harness = ReturnType<typeof setup>;

/** Resolves once the injected sleep is entered; the returned sleep never resolves on its own. */
function parkInSleep(): { entered: Promise<void>; sleep: (ms: number) => Promise<void> } {
  let enter: () => void = () => {};
  const entered = new Promise<void>((resolve) => {
    enter = resolve;
  });
  return {
    entered,
    sleep: () => {
      enter();
      return new Promise<void>(() => {});
    },
  };
}

// Compile-time check: pollInterval must be a number. Never called; tsc checks the @ts-expect-error line.
function typeChecks(dialect: MemoryDialect): void {
  new Worker({
    dialect,
    jobs,
    context,
    // @ts-expect-error pollInterval must be a number, not a string
    pollInterval: "1000",
  });
}

describe("Worker run loop", () => {
  beforeEach(() => {
    log.length = 0;
    failing = new Set<string>();
    onRun = () => {};
    onSleep = () => Promise.resolve();
  });

  it("processes N jobs in order, then waits pollInterval on an empty queue and stops when stop() is called", async () => {
    const h: Harness = setup({ pollInterval: 250 });
    onSleep = () => {
      h.worker.stop();
      return Promise.resolve();
    };
    for (const to of ["a", "b", "c"]) await h.queue.enqueue(new Mail({ to }));

    await h.worker.run();

    expect(log).toEqual(["run:a", "run:b", "run:c", "sleep"]);
    expect(h.sleeps).toEqual([250]);
  });

  it("stop() during a handler: the in-flight job is acknowledged, run() resolves, next message is not processed", async () => {
    const h = setup();
    onRun = (to) => {
      if (to === "a") h.worker.stop();
    };
    await h.queue.enqueue(new Mail({ to: "a" }));
    await h.queue.enqueue(new Mail({ to: "b" }));

    await h.worker.run();

    expect(log).toEqual(["run:a"]);
    await expect(h.dialect.count("default", 1_000)).resolves.toEqual({ ready: 1, delayed: 0, reserved: 0 });
    expect(h.sleeps).toEqual([]);
  });

  it("stop() during an idle wait ends the wait, run() resolves, and no further dequeue happens", async () => {
    const h = setup({ pollInterval: 5_000 });
    const parked = parkInSleep();
    onSleep = parked.sleep;
    const dequeueSpy = vi.spyOn(h.dialect, "dequeue");

    const done = h.worker.run();
    await parked.entered;
    h.worker.stop();
    await done;

    expect(dequeueSpy).toHaveBeenCalledTimes(1);
    expect(h.sleeps).toEqual([5_000]);
  });

  it("stop() before run(): run() resolves immediately without dequeuing", async () => {
    const h = setup();
    await h.queue.enqueue(new Mail({ to: "a" }));
    const dequeueSpy = vi.spyOn(h.dialect, "dequeue");

    h.worker.stop();

    await expect(h.worker.run()).resolves.toBeUndefined();
    expect(dequeueSpy).not.toHaveBeenCalled();
    expect(log).toEqual([]);
  });

  it("uses a default pollInterval of 1000 ms when none is set", async () => {
    const h = setup();
    onSleep = () => {
      h.worker.stop();
      return Promise.resolve();
    };

    await h.worker.run();

    expect(h.sleeps).toEqual([1_000]);
  });

  it.each([
    ["zero", 0],
    ["negative", -1],
    ["NaN", Number.NaN],
    ["Infinity", Number.POSITIVE_INFINITY],
  ])("throws in the constructor for pollInterval %s", (_label, value) => {
    expect(() => setup({ pollInterval: value })).toThrow(/pollInterval/);
  });

  it("a handler error does not reject run(): the message is released per TS-78 and the loop processes the next message", async () => {
    const h = setup();
    failing.add("a");
    onSleep = () => {
      h.worker.stop();
      return Promise.resolve();
    };
    await h.queue.enqueue(new Mail({ to: "a" }));
    await h.queue.enqueue(new Mail({ to: "b" }));

    await expect(h.worker.run()).resolves.toBeUndefined();

    expect(log).toEqual(["run:a", "run:b", "sleep"]);
    await expect(h.dialect.count("default", 1_000)).resolves.toEqual({ ready: 0, delayed: 1, reserved: 0 });
    expect(h.sleeps).toEqual([1_000]);
  });

  it("run() called while active throws; a run() after the first resolves does not throw", async () => {
    const h = setup();
    const parked = parkInSleep();
    onSleep = parked.sleep;

    const first = h.worker.run();
    await parked.entered;
    await expect(h.worker.run()).rejects.toThrow(/already running/);
    h.worker.stop();
    await first;

    await expect(h.worker.run()).resolves.toBeUndefined();
  });

  it("a poison envelope ahead of a good message: the good one runs and the poison is dropped after tries", async () => {
    const h = setup();
    await h.dialect.enqueue("default", JSON.stringify({ job: "Nope", payload: {} }), { availableAt: 0, now: 0 });
    await h.queue.enqueue(new Mail({ to: "a" }));
    onSleep = () => {
      h.clock.now += 1_000;
      if (h.clock.now >= 5_000) h.worker.stop();
      return Promise.resolve();
    };

    await expect(h.worker.run()).resolves.toBeUndefined();

    expect(log).toEqual(["run:a", "sleep", "sleep", "sleep", "sleep"]);
    expect(h.sleeps).toEqual([1_000, 1_000, 1_000, 1_000]);
    await expect(h.dialect.count("default", 5_000)).resolves.toEqual({ ready: 0, delayed: 0, reserved: 0 });
  });

  it("a poison envelope is redelivered at each backoff availableAt and gone after tries (MemoryDialect, injected clock)", async () => {
    const dialect = new MemoryDialect();
    const clock = { now: 1_000 };
    const worker = new Worker({
      dialect,
      jobs,
      context,
      now: () => clock.now,
      lease: 100,
      sleep: () => Promise.resolve(),
    });
    await dialect.enqueue("default", JSON.stringify({ job: "Nope", payload: {} }), { availableAt: 0, now: 0 });

    // Attempt 1 at 1000 releases to 2000; attempt 2 at 2000 releases to 4000; attempt 3 at 4000 acks.
    await expect(worker.processOne()).rejects.toThrow(/Nope/);
    clock.now = 2_000;
    await expect(worker.processOne()).rejects.toThrow(/Nope/);
    clock.now = 4_000;
    await expect(worker.processOne()).rejects.toThrow(/Nope/);

    clock.now = 1_000_000;
    await expect(dialect.count("default", clock.now)).resolves.toEqual({ ready: 0, delayed: 0, reserved: 0 });
    await expect(worker.processOne()).resolves.toBe(false);
  });

  it("stop() called twice does not throw", async () => {
    const h = setup();
    expect(() => {
      h.worker.stop();
      h.worker.stop();
    }).not.toThrow();
    await expect(h.worker.run()).resolves.toBeUndefined();
  });

  it("default sleep waits pollInterval via setTimeout, and stop() ends the wait early", async () => {
    vi.useFakeTimers();
    try {
      const dialect = new MemoryDialect();
      const worker = new Worker({ dialect, jobs, context, now: () => 1_000, pollInterval: 5_000 });
      const done = worker.run();
      await vi.advanceTimersByTimeAsync(4_999);
      let settled = false;
      void done.then(() => {
        settled = true;
      });
      await vi.advanceTimersByTimeAsync(0);
      expect(settled).toBe(false);
      worker.stop();
      await done;
    } finally {
      vi.useRealTimers();
    }
  });

  it("a job that ran does not trigger a sleep before the next dequeue", async () => {
    const h = setup();
    onSleep = () => {
      h.worker.stop();
      return Promise.resolve();
    };
    await h.queue.enqueue(new Mail({ to: "a" }));
    await h.queue.enqueue(new Mail({ to: "b" }));

    await h.worker.run();

    expect(log).toEqual(["run:a", "run:b", "sleep"]);
  });
});
