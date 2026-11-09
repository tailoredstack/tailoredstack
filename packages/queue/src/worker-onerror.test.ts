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

const boom = new Error("boom");
const dequeueDown = new Error("dequeue down");
const releaseDown = new Error("release down");
const ackDown = new Error("ack down");
const clockDown = new Error("clock down");

const log: string[] = [];
let failing = new Set<string>();
let onRun: (to: string) => void = () => {};
let onSleep: () => Promise<void> = () => Promise.resolve();

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

function setup(options: { onError?: (error: unknown) => void; tries?: number } = {}) {
  const dialect = new MemoryDialect();
  const clock = { now: 1_000, broken: false };
  const queue = new Queue(dialect, { jobs, now: () => clock.now });
  const sleeps: number[] = [];
  const worker = new Worker({
    dialect,
    jobs,
    context,
    now: () => {
      if (clock.broken) throw clockDown;
      return clock.now;
    },
    sleep: (ms) => {
      sleeps.push(ms);
      log.push("sleep");
      return onSleep();
    },
    ...options,
  });
  return { dialect, queue, worker, sleeps, clock };
}

type Harness = ReturnType<typeof setup>;

// Compile-time check: onError must be a function. Never called; tsc checks the @ts-expect-error line.
function typeChecks(dialect: Dialect): void {
  new Worker({
    dialect,
    jobs,
    context,
    // @ts-expect-error onError must be a function, not a string
    onError: "not a function",
  });
  new Worker({
    dialect,
    jobs,
    context,
    onError: (error: unknown) => {
      void error;
    },
  });
}

describe("Worker onError routing for dialect errors", () => {
  beforeEach(() => {
    log.length = 0;
    failing = new Set<string>();
    onRun = () => {};
    onSleep = () => Promise.resolve();
  });

  it("dequeue rejects: onError gets the exact error, handle is not called, run() resolves, and the idle wait happens", async () => {
    const onError = vi.fn();
    const h: Harness = setup({ onError });
    const handleSpy = vi.fn();
    onRun = handleSpy;
    onSleep = () => {
      h.worker.stop();
      return Promise.resolve();
    };
    await h.queue.enqueue(new Mail({ to: "a" }));
    vi.spyOn(h.dialect, "dequeue").mockRejectedValueOnce(dequeueDown);

    await expect(h.worker.run()).resolves.toBeUndefined();

    expect(onError).toHaveBeenCalledTimes(1);
    expect(onError).toHaveBeenCalledWith(dequeueDown);
    expect(onError.mock.calls[0]?.[0]).toBe(dequeueDown);
    expect(handleSpy).not.toHaveBeenCalled();
    expect(h.sleeps).toEqual([1_000]);
  });

  it("a dialect error never reaches handle: a dequeue rejection leaves the handle call count at 0", async () => {
    const onError = vi.fn();
    const h = setup({ onError });
    const handleSpy = vi.fn();
    onRun = handleSpy;
    onSleep = () => {
      h.worker.stop();
      return Promise.resolve();
    };
    await h.queue.enqueue(new Mail({ to: "a" }));
    vi.spyOn(h.dialect, "dequeue").mockRejectedValueOnce(dequeueDown);

    await h.worker.run();

    expect(handleSpy).toHaveBeenCalledTimes(0);
    expect(log).toEqual(["sleep"]);
  });

  it("release rejects during a retry: onError gets the release error, processOne rejects with the original handle error, handle is called once", async () => {
    const onError = vi.fn();
    const h = setup({ onError });
    const handleSpy = vi.fn();
    onRun = handleSpy;
    failing.add("a");
    await h.queue.enqueue(new Mail({ to: "a" }));
    vi.spyOn(h.dialect, "release").mockRejectedValueOnce(releaseDown);

    const outcome = h.worker.processOne();

    await expect(outcome).rejects.toBe(boom);
    expect(onError).toHaveBeenCalledTimes(1);
    expect(onError.mock.calls[0]?.[0]).toBe(releaseDown);
    expect(handleSpy).toHaveBeenCalledTimes(1);
  });

  it("acknowledge rejects on the success path: onError gets the error, the handle has run, and run() does not reject", async () => {
    const onError = vi.fn();
    const h = setup({ onError });
    onSleep = () => {
      h.worker.stop();
      return Promise.resolve();
    };
    await h.queue.enqueue(new Mail({ to: "a" }));
    vi.spyOn(h.dialect, "acknowledge").mockRejectedValueOnce(ackDown);

    await expect(h.worker.run()).resolves.toBeUndefined();

    expect(log).toContain("run:a");
    expect(onError).toHaveBeenCalledTimes(1);
    expect(onError.mock.calls[0]?.[0]).toBe(ackDown);
  });

  it("acknowledge rejects on the final failed attempt: onError gets the error, and the handle error still propagates from processOne", async () => {
    const onError = vi.fn();
    const h = setup({ onError, tries: 1 });
    failing.add("a");
    await h.queue.enqueue(new Mail({ to: "a" }));
    vi.spyOn(h.dialect, "acknowledge").mockRejectedValueOnce(ackDown);

    await expect(h.worker.processOne()).rejects.toBe(boom);

    expect(onError).toHaveBeenCalledTimes(1);
    expect(onError.mock.calls[0]?.[0]).toBe(ackDown);
  });

  it("handle throws: onError is NOT called, in processOne or in run()", async () => {
    const onError = vi.fn();
    const h = setup({ onError });
    failing.add("a");
    onSleep = () => {
      h.worker.stop();
      return Promise.resolve();
    };
    await h.queue.enqueue(new Mail({ to: "a" }));

    await h.worker.run();

    expect(onError).not.toHaveBeenCalled();
  });

  it("handle throws on processOne: onError is NOT called and the handle error propagates", async () => {
    const onError = vi.fn();
    const h = setup({ onError });
    failing.add("a");
    await h.queue.enqueue(new Mail({ to: "a" }));

    await expect(h.worker.processOne()).rejects.toBe(boom);
    expect(onError).not.toHaveBeenCalled();
  });

  it("a failure-path clock throw is discarded: onError is NOT called and the handle error still propagates", async () => {
    const onError = vi.fn();
    const h = setup({ onError });
    failing.add("a");
    onRun = () => {
      h.clock.broken = true;
    };
    await h.queue.enqueue(new Mail({ to: "a" }));

    await expect(h.worker.processOne()).rejects.toBe(boom);
    expect(onError).not.toHaveBeenCalled();
  });

  it("onError throwing does not stop run(): run() keeps going and processes the next message", async () => {
    const onError = vi.fn(() => {
      throw new Error("onError exploded");
    });
    const h = setup({ onError });
    onRun = (to) => {
      if (to === "a") h.worker.stop();
    };
    vi.spyOn(h.dialect, "dequeue").mockRejectedValueOnce(dequeueDown);
    await h.queue.enqueue(new Mail({ to: "a" }));

    await expect(h.worker.run()).resolves.toBeUndefined();

    expect(onError).toHaveBeenCalledTimes(1);
    expect(log).toEqual(["sleep", "run:a"]);
  });

  it("default with no onError: dialect errors are dropped silently and run() does not reject", async () => {
    const h = setup();
    onSleep = () => {
      h.worker.stop();
      return Promise.resolve();
    };
    await h.queue.enqueue(new Mail({ to: "a" }));
    vi.spyOn(h.dialect, "dequeue").mockRejectedValueOnce(dequeueDown);

    await expect(h.worker.run()).resolves.toBeUndefined();
    expect(h.sleeps).toEqual([1_000]);
  });

  it("processOne still throws a dequeue error to its caller; only run() routes it to onError", async () => {
    const onError = vi.fn();
    const h = setup({ onError });
    vi.spyOn(h.dialect, "dequeue").mockRejectedValueOnce(dequeueDown);

    await expect(h.worker.processOne()).rejects.toBe(dequeueDown);
    expect(onError).not.toHaveBeenCalled();
  });

  it("typed onError: a non-function fails to compile (checked by tsc via @ts-expect-error)", () => {
    expect(typeChecks).toBeTypeOf("function");
  });
});
