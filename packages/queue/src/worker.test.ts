import { describe, expect, it, vi } from "vitest";
import type { Dialect } from "./dialect.ts";
import type { JobContext } from "./job.ts";
import { Job } from "./job.ts";
import { MemoryDialect } from "./memory-dialect.ts";
import type { JobRegistry } from "./queue.ts";
import { Queue } from "./queue.ts";
import { Worker } from "./worker.ts";

interface MailPayload {
  readonly to: string;
}

interface AppContext {
  readonly tenant: string;
}

const seen: Array<{ to: string; ctx: JobContext<AppContext> }> = [];

class SendMail extends Job<MailPayload, AppContext> {
  declare readonly __jobKey: "SendMail";

  async handle(ctx: JobContext<AppContext>): Promise<void> {
    seen.push({ to: this.payload.to, ctx });
  }
}

class Failing extends Job<MailPayload, AppContext> {
  declare readonly __jobKey: "Failing";

  async handle(_ctx: JobContext<AppContext>): Promise<void> {
    throw new Error("boom");
  }
}

const jobs = { SendMail, Failing };
const context: AppContext = { tenant: "acme" };

function setup(now: () => number = () => 1_000) {
  const dialect = new MemoryDialect();
  const queue = new Queue(dialect, { jobs, now });
  const worker = new Worker({ dialect, jobs, context, now });
  return { dialect, queue, worker };
}

async function rawEnqueue(dialect: Dialect, body: string): Promise<void> {
  await dialect.enqueue("default", body, { availableAt: 0, now: 0 });
}

describe("Worker", () => {
  it("processes N jobs in enqueue order, each handle seeing its own payload", async () => {
    seen.length = 0;
    const { queue, worker } = setup();
    for (const to of ["a", "b", "c"]) await queue.enqueue(new SendMail({ to }));

    expect(await worker.processOne()).toBe(true);
    expect(await worker.processOne()).toBe(true);
    expect(await worker.processOne()).toBe(true);

    expect(seen.map((s) => s.to)).toEqual(["a", "b", "c"]);
  });

  it("gives each run the context fields plus a distinct signal", async () => {
    seen.length = 0;
    const { queue, worker } = setup();
    await queue.enqueue(new SendMail({ to: "a" }));
    await queue.enqueue(new SendMail({ to: "b" }));
    await worker.processOne();
    await worker.processOne();

    expect(seen[0]?.ctx.tenant).toBe("acme");
    expect(seen[0]?.ctx.signal).toBeInstanceOf(AbortSignal);
    expect(seen[0]?.ctx.signal).not.toBe(seen[1]?.ctx.signal);
  });

  it("returns false on an empty queue without calling any handle", async () => {
    seen.length = 0;
    const { worker } = setup();
    await expect(worker.processOne()).resolves.toBe(false);
    expect(seen).toHaveLength(0);
  });

  it("acknowledges a successful job so nothing is ready or reserved", async () => {
    seen.length = 0;
    const { dialect, queue, worker } = setup();
    await queue.enqueue(new SendMail({ to: "a" }));
    await worker.processOne();
    await expect(dialect.count("default", 1_000)).resolves.toEqual({ ready: 0, delayed: 0, reserved: 0 });
  });

  it("propagates a throwing handle and releases the message with backoff, then redeliverable at availableAt", async () => {
    let clock = 1_000;
    const dialect = new MemoryDialect();
    const queue = new Queue(dialect, { jobs, now: () => clock });
    const worker = new Worker({ dialect, jobs, context, now: () => clock, lease: 500 });
    await queue.enqueue(new Failing({ to: "a" }));

    await expect(worker.processOne()).rejects.toThrow("boom");
    await expect(dialect.count("default", clock)).resolves.toEqual({ ready: 0, delayed: 1, reserved: 0 });

    clock = 2_000;
    const redelivered = await dialect.dequeue("default", { now: clock, leaseUntil: clock + 500 });
    expect(redelivered).not.toBeNull();
    expect(redelivered?.attempts).toBe(2);
  });

  it("rethrows an unknown registry key and releases it with backoff on attempt 1", async () => {
    const dialect = new MemoryDialect();
    const worker = new Worker({ dialect, jobs, context, now: () => 1_000 });
    await rawEnqueue(dialect, JSON.stringify({ job: "Nope", payload: { to: "a" } }));

    await expect(worker.processOne()).rejects.toThrow(/Nope/);
    await expect(dialect.count("default", 1_000)).resolves.toEqual({ ready: 0, delayed: 1, reserved: 0 });
    await expect(dialect.count("default", 2_000)).resolves.toEqual({ ready: 1, delayed: 0, reserved: 0 });
  });

  it.each([
    ["not JSON", "{not json"],
    ["missing job", JSON.stringify({ payload: { to: "a" } })],
    ["missing payload", JSON.stringify({ job: "SendMail" })],
  ])("rethrows a malformed envelope (%s) and releases it with backoff on attempt 1", async (_label, body) => {
    const dialect = new MemoryDialect();
    const worker = new Worker({ dialect, jobs, context, now: () => 1_000 });
    await rawEnqueue(dialect, body);

    await expect(worker.processOne()).rejects.toThrow(/envelope|JSON/i);
    await expect(dialect.count("default", 1_000)).resolves.toEqual({ ready: 0, delayed: 1, reserved: 0 });
  });

  it.each([
    ["malformed envelope", "{not json", /JSON/],
    ["unknown key", JSON.stringify({ job: "Nope", payload: {} }), /Nope/],
  ])("acknowledges a %s once attempts equals tries", async (_label, body, error) => {
    const dialect = new MemoryDialect();
    const worker = new Worker({ dialect, jobs, context, now: () => 1_000, tries: 1 });
    await rawEnqueue(dialect, body);

    await expect(worker.processOne()).rejects.toThrow(error);
    await expect(dialect.count("default", 1_000)).resolves.toEqual({ ready: 0, delayed: 0, reserved: 0 });
  });

  it("respects a custom queue name", async () => {
    seen.length = 0;
    const dialect = new MemoryDialect();
    const queue = new Queue(dialect, { jobs, queue: "mail", now: () => 1_000 });
    const worker = new Worker({ dialect, jobs, context, queue: "mail", now: () => 1_000 });
    await queue.enqueue(new SendMail({ to: "a" }));

    expect(await new Worker({ dialect, jobs, context, now: () => 1_000 }).processOne()).toBe(false);
    expect(await worker.processOne()).toBe(true);
    expect(seen.map((s) => s.to)).toEqual(["a"]);
  });

  it("uses a 30000 ms default lease for dequeue", async () => {
    const dialect = new MemoryDialect();
    const spy = vi.spyOn(dialect, "dequeue");
    const queue = new Queue(dialect, { jobs, now: () => 5_000 });
    const worker = new Worker({ dialect, jobs, context, now: () => 5_000 });
    await queue.enqueue(new SendMail({ to: "a" }));

    await worker.processOne();
    expect(spy).toHaveBeenCalledWith("default", { now: 5_000, leaseUntil: 35_000 });
  });

  it("rejects a non-string job key and releases it with backoff on attempt 1", async () => {
    const dialect = new MemoryDialect();
    const worker = new Worker({ dialect, jobs, context, now: () => 1_000 });
    await rawEnqueue(dialect, JSON.stringify({ job: 42, payload: {} }));

    await expect(worker.processOne()).rejects.toThrow(/job must be a string/);
    await expect(dialect.count("default", 1_000)).resolves.toEqual({ ready: 0, delayed: 1, reserved: 0 });
  });

  it("falls back to Date.now when no clock is injected", async () => {
    const dialect = new MemoryDialect();
    const dequeue = vi.spyOn(dialect, "dequeue");
    const queue = new Queue(dialect, { jobs });
    const worker = new Worker({ dialect, jobs, context });
    await queue.enqueue(new SendMail({ to: "a" }));

    vi.spyOn(Date, "now").mockReturnValue(9_000);
    await worker.processOne();
    vi.restoreAllMocks();
    expect(dequeue).toHaveBeenCalledWith("default", { now: 9_000, leaseUntil: 39_000 });
  });

  it("uses the injected now for dequeue and acknowledge", async () => {
    const dialect = new MemoryDialect();
    const dequeue = vi.spyOn(dialect, "dequeue");
    const acknowledge = vi.spyOn(dialect, "acknowledge");
    const queue = new Queue(dialect, { jobs, now: () => 7_000 });
    const worker = new Worker({ dialect, jobs, context, now: () => 7_000, lease: 100 });
    await queue.enqueue(new SendMail({ to: "a" }));

    await worker.processOne();
    expect(dequeue).toHaveBeenCalledWith("default", { now: 7_000, leaseUntil: 7_100 });
    expect(acknowledge).toHaveBeenCalledWith("default", expect.any(String), 7_000);
  });

  it("gives a job reading ctx.mailer the mailer when the context has it", async () => {
    const mailed: string[] = [];
    class NeedsMailer extends Job<MailPayload, { db: string; mailer: string }> {
      declare readonly __jobKey: "NeedsMailer";

      async handle(ctx: JobContext<{ db: string; mailer: string }>): Promise<void> {
        mailed.push(ctx.mailer);
      }
    }
    const registry = { NeedsMailer };
    const dialect = new MemoryDialect();
    const queue = new Queue(dialect, { jobs: registry, now: () => 1_000 });
    const worker = new Worker({
      dialect,
      jobs: registry,
      context: { db: "d", mailer: "smtp" },
      now: () => 1_000,
    });
    await queue.enqueue(new NeedsMailer({ to: "a" }));

    await worker.processOne();
    expect(mailed).toEqual(["smtp"]);
  });

  it("does not let a context __proto__ key change the job's ctx prototype", async () => {
    const seenCtx: Array<Record<string, unknown>> = [];
    class Probe extends Job<MailPayload, Record<string, unknown>> {
      declare readonly __jobKey: "Probe";

      async handle(ctx: JobContext<Record<string, unknown>>): Promise<void> {
        seenCtx.push(ctx);
      }
    }
    const registry = { Probe };
    const dialect = new MemoryDialect();
    const queue = new Queue(dialect, { jobs: registry, now: () => 1_000 });
    const hostile = JSON.parse('{"__proto__": {"polluted": true}}') as Record<string, unknown>;
    const worker = new Worker({ dialect, jobs: registry, context: hostile, now: () => 1_000 });
    await queue.enqueue(new Probe({ to: "a" }));

    await worker.processOne();
    expect(seenCtx[0]?.polluted).toBeUndefined();
    expect(Object.getPrototypeOf(seenCtx[0] ?? {})).toBeNull();
  });

  it("throws at construction when one class is registered under two keys", () => {
    const dialect = new MemoryDialect();
    const dup = { SendMail, Alias: SendMail } as unknown as JobRegistry;
    expect(() => new Worker({ dialect, jobs: dup, context: {} as never })).toThrow(/SendMail.*"SendMail".*"Alias"/);
  });

  describe("compile-time checks", () => {
    it("rejects wrong context types and bad handle shapes", () => {
      // Nothing here runs: the function exists so tsc checks each @ts-expect-error line.
      const compileOnly = (dialect: Dialect) => {
        // @ts-expect-error context.tenant must be a string, not a number
        new Worker({ dialect, jobs, context: { tenant: 1 } });
        new Worker({ dialect, jobs, context });
      };
      expect(compileOnly).toBeTypeOf("function");
    });

    it("requires context to satisfy the intersection of every job's Ctx", () => {
      class NeedsDb extends Job<MailPayload, { db: string }> {
        declare readonly __jobKey: "NeedsDb";

        async handle(ctx: JobContext<{ db: string }>): Promise<void> {
          void ctx;
        }
      }
      class NeedsDbMailer extends Job<MailPayload, { db: string; mailer: string }> {
        declare readonly __jobKey: "NeedsDbMailer";

        async handle(ctx: JobContext<{ db: string; mailer: string }>): Promise<void> {
          void ctx.mailer;
        }
      }
      const compileOnly = (dialect: Dialect) => {
        // @ts-expect-error context lacks mailer, which NeedsDbMailer requires
        new Worker({ dialect, jobs: { NeedsDb, NeedsDbMailer }, context: { db: "x" } });
      };
      expect(compileOnly).toBeTypeOf("function");
    });

    it("rejects a registry key that does not match the class brand", () => {
      const compileOnly = (dialect: Dialect) => {
        // @ts-expect-error Alias is not SendMail's brand key
        new Worker({ dialect, jobs: { Alias: SendMail }, context });
      };
      expect(compileOnly).toBeTypeOf("function");
    });
  });
});
