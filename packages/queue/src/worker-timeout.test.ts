import { afterEach, describe, expect, it, vi } from "vitest";
import { Job, type JobContext } from "./job.ts";
import { MemoryDialect } from "./memory-dialect.ts";
import { Queue } from "./queue.ts";
import { JobTimeoutError, Worker } from "./worker.ts";

interface Payload {
  readonly to: string;
}

const hangs = vi.fn();
class Hang extends Job<Payload, object> {
  declare readonly __jobKey: "Hang";

  async handle(_ctx: JobContext<object>): Promise<void> {
    hangs(this.payload.to);
    await new Promise<never>(() => {});
  }
}

const seenReasons: unknown[] = [];
class Cooperative extends Job<Payload, object> {
  declare readonly __jobKey: "Cooperative";

  async handle(ctx: JobContext<object>): Promise<void> {
    await new Promise<void>((resolve) => {
      ctx.signal.addEventListener("abort", () => {
        seenReasons.push(ctx.signal.reason);
        resolve();
      });
    });
    throw new Error("cancelled");
  }
}

class Quick extends Job<Payload, object> {
  declare readonly __jobKey: "Quick";

  async handle(_ctx: JobContext<object>): Promise<void> {}
}

const lateSettled: string[] = [];
class LateFail extends Job<Payload, object> {
  declare readonly __jobKey: "LateFail";

  async handle(_ctx: JobContext<object>): Promise<void> {
    await new Promise((resolve) => setTimeout(resolve, 40));
    lateSettled.push("rejected");
    throw new Error("too late");
  }
}

const orphanFinished: string[] = [];
class Orphan extends Job<Payload, object> {
  declare readonly __jobKey: "Orphan";

  async handle(_ctx: JobContext<object>): Promise<void> {
    await new Promise((resolve) => setTimeout(resolve, 40));
    orphanFinished.push(this.payload.to);
  }
}

const jobs = { Hang, Cooperative, Quick, LateFail, Orphan };
const context = {};

afterEach(() => {
  vi.useRealTimers();
  hangs.mockClear();
  seenReasons.length = 0;
  lateSettled.length = 0;
  orphanFinished.length = 0;
});

describe("Worker timeout", () => {
  it("aborts a hanging handler after timeout ms and treats it as a failure", async () => {
    const dialect = new MemoryDialect();
    const queue = new Queue(dialect, { jobs, now: () => 1_000 });
    const worker = new Worker({ dialect, jobs, context, now: () => 1_000, lease: 100, timeout: 20 });
    await queue.enqueue(new Hang({ to: "a" }));

    await expect(worker.processOne()).rejects.toThrow("Job timed out after 20 ms");
    await expect(dialect.count("default", 1_000)).resolves.toEqual({ ready: 0, delayed: 1, reserved: 0 });
  });

  it("retries a timed-out run: the message is redelivered with attempts incremented", async () => {
    const dialect = new MemoryDialect();
    const queue = new Queue(dialect, { jobs, now: () => 1_000 });
    const worker = new Worker({ dialect, jobs, context, now: () => 1_000, lease: 100, timeout: 20 });
    await queue.enqueue(new Hang({ to: "a" }));

    await expect(worker.processOne()).rejects.toThrow(/timed out/);
    const redelivered = await dialect.dequeue("default", { now: 2_000, leaseUntil: 2_100 });
    expect(redelivered?.attempts).toBe(2);
    expect(hangs).toHaveBeenCalledTimes(1);
  });

  it("drops a message whose every attempt times out, once tries is exhausted", async () => {
    let clock = 0;
    const dialect = new MemoryDialect();
    const queue = new Queue(dialect, { jobs, now: () => clock });
    const worker = new Worker({ dialect, jobs, context, now: () => clock, lease: 100, timeout: 10, tries: 2 });
    await queue.enqueue(new Hang({ to: "a" }));

    await expect(worker.processOne()).rejects.toThrow(/timed out/);
    clock = 1_000;
    await expect(worker.processOne()).rejects.toThrow(/timed out/);
    clock = 1_000_000;
    await expect(dialect.count("default", clock)).resolves.toEqual({ ready: 0, delayed: 0, reserved: 0 });
  });

  it("aborts the signal with the timeout error for a handler that honors it", async () => {
    const dialect = new MemoryDialect();
    const queue = new Queue(dialect, { jobs, now: () => 1_000 });
    const worker = new Worker({ dialect, jobs, context, now: () => 1_000, lease: 100, timeout: 20 });
    await queue.enqueue(new Cooperative({ to: "a" }));

    await expect(worker.processOne()).rejects.toThrow("Job timed out after 20 ms");
    expect(seenReasons).toHaveLength(1);
    expect(seenReasons[0]).toBeInstanceOf(Error);
    expect((seenReasons[0] as Error).message).toBe("Job timed out after 20 ms");
  });

  it("does not time out a fast handler and leaves no timer behind", async () => {
    vi.useFakeTimers();
    const dialect = new MemoryDialect();
    const queue = new Queue(dialect, { jobs, now: () => 0 });
    const worker = new Worker({ dialect, jobs, context, now: () => 0 });
    await queue.enqueue(new Quick({ to: "a" }));

    await expect(worker.processOne()).resolves.toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("ignores a late rejection after the timeout has already fired", async () => {
    const dialect = new MemoryDialect();
    const queue = new Queue(dialect, { jobs, now: () => 1_000 });
    const worker = new Worker({ dialect, jobs, context, now: () => 1_000, lease: 100, timeout: 10 });
    await queue.enqueue(new LateFail({ to: "a" }));

    await expect(worker.processOne()).rejects.toThrow("Job timed out after 10 ms");
    expect(lateSettled).toEqual([]);
    await new Promise((resolve) => setTimeout(resolve, 60));
    expect(lateSettled).toEqual(["rejected"]);
  });

  it("throws a JobTimeoutError that callers can match by class and carries the timeout", async () => {
    const dialect = new MemoryDialect();
    const queue = new Queue(dialect, { jobs, now: () => 1_000 });
    const worker = new Worker({ dialect, jobs, context, now: () => 1_000, lease: 100, timeout: 20 });
    await queue.enqueue(new Hang({ to: "a" }));

    const error = await worker.processOne().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(JobTimeoutError);
    expect((error as JobTimeoutError).name).toBe("JobTimeoutError");
    expect((error as JobTimeoutError).timeoutMs).toBe(20);
  });

  it("does not kill a handler that ignores its signal: it keeps running after the run is released", async () => {
    const dialect = new MemoryDialect();
    const queue = new Queue(dialect, { jobs, now: () => 1_000 });
    const worker = new Worker({ dialect, jobs, context, now: () => 1_000, lease: 100, timeout: 10 });
    await queue.enqueue(new Orphan({ to: "a" }));

    await expect(worker.processOne()).rejects.toBeInstanceOf(JobTimeoutError);
    expect(orphanFinished).toEqual([]);
    await new Promise((resolve) => setTimeout(resolve, 60));
    expect(orphanFinished).toEqual(["a"]);
  });

  it("defaults the timeout to half the default 30 s lease (15 s)", async () => {
    vi.useFakeTimers();
    const dialect = new MemoryDialect();
    const queue = new Queue(dialect, { jobs, now: () => 0 });
    const worker = new Worker({ dialect, jobs, context, now: () => 0 });
    await queue.enqueue(new Hang({ to: "a" }));

    const run = worker.processOne().catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(14_999);
    expect(hangs).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(1);
    const error = (await run) as JobTimeoutError;
    expect(error).toBeInstanceOf(JobTimeoutError);
    expect(error.message).toBe("Job timed out after 15000 ms");
  });

  it("caps the default timeout at 25 s for leases over 50 s", async () => {
    vi.useFakeTimers();
    const dialect = new MemoryDialect();
    const queue = new Queue(dialect, { jobs, now: () => 0 });
    const worker = new Worker({ dialect, jobs, context, now: () => 0, lease: 60_000 });
    await queue.enqueue(new Hang({ to: "a" }));

    const run = worker.processOne().catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(24_999);
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(1);
    const error = (await run) as JobTimeoutError;
    expect(error).toBeInstanceOf(JobTimeoutError);
    expect(error.message).toBe("Job timed out after 25000 ms");
  });

  it("defaults the timeout to half a short lease", async () => {
    vi.useFakeTimers();
    const dialect = new MemoryDialect();
    const queue = new Queue(dialect, { jobs, now: () => 0 });
    const worker = new Worker({ dialect, jobs, context, now: () => 0, lease: 10_000 });
    await queue.enqueue(new Hang({ to: "a" }));

    const run = worker.processOne().catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(4_999);
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(1);
    const error = (await run) as JobTimeoutError;
    expect(error).toBeInstanceOf(JobTimeoutError);
    expect(error.message).toBe("Job timed out after 5000 ms");
  });

  it.each([
    ["zero", 0],
    ["negative", -5],
    ["NaN", Number.NaN],
    ["Infinity", Number.POSITIVE_INFINITY],
  ])("rejects a %s timeout at construction", (_label, timeout) => {
    const dialect = new MemoryDialect();
    expect(() => new Worker({ dialect, jobs, context, timeout })).toThrow(/Invalid timeout/);
  });

  it("rejects timeout equal to lease at construction", () => {
    const dialect = new MemoryDialect();
    expect(() => new Worker({ dialect, jobs, context, lease: 500, timeout: 500 })).toThrow(
      /must be less than lease 500/,
    );
  });

  it("rejects timeout greater than the default 30 s lease at construction", () => {
    const dialect = new MemoryDialect();
    expect(() => new Worker({ dialect, jobs, context, timeout: 30_000 })).toThrow(/must be less than lease 30000/);
  });

  it("rejects a fractional timeout at construction", () => {
    const dialect = new MemoryDialect();
    expect(() => new Worker({ dialect, jobs, context, lease: 500, timeout: 10.5 })).toThrow(/integer number of ms/);
  });

  it.each([
    ["zero", 0],
    ["negative", -1],
    ["NaN", Number.NaN],
    ["Infinity", Number.POSITIVE_INFINITY],
  ])("rejects a %s lease at construction, naming the lease", (_label, lease) => {
    const dialect = new MemoryDialect();
    expect(() => new Worker({ dialect, jobs, context, lease })).toThrow(/Invalid lease/);
  });

  it("floors the default timeout for an odd lease", () => {
    const dialect = new MemoryDialect();
    expect(() => new Worker({ dialect, jobs, context, lease: 101 })).not.toThrow();
  });

  it("accepts timeout just under lease", () => {
    const dialect = new MemoryDialect();
    expect(() => new Worker({ dialect, jobs, context, lease: 500, timeout: 499 })).not.toThrow();
  });
});
