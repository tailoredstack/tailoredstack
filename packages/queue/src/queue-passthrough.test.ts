import { describe, expect, expectTypeOf, it, vi } from "vitest";
import type { Dialect, DialectCapabilities, Message, QueueCount, Receipt } from "./dialect.ts";
import { Job, type JobContext } from "./job.ts";
import { MemoryDialect } from "./memory-dialect.ts";
import { Queue } from "./queue.ts";

class SendMail extends Job<{ readonly to: string }> {
  declare readonly __jobKey: "SendMail";

  async handle(_ctx: JobContext<unknown>): Promise<void> {}
}

const MAX_DELAY_MS = 60_000;
const MAX_LEASE_MS = 60_000;

/**
 * Wraps a real MemoryDialect with spied methods and overridable capabilities. Every call
 * reaches the MemoryDialect, so behavior is real; the spies only prove whether a call happened.
 */
function spiedDialect(capabilities: Partial<DialectCapabilities> = {}) {
  const inner = new MemoryDialect();
  const spies = {
    enqueue: vi.fn((name: string, payload: string, at: { availableAt: number; now: number }) =>
      inner.enqueue(name, payload, at),
    ),
    dequeue: vi.fn((name: string, at: { now: number; leaseUntil: number }) => inner.dequeue(name, at)),
    acknowledge: vi.fn((name: string, receipt: Receipt, now: number) => inner.acknowledge(name, receipt, now)),
    release: vi.fn((name: string, receipt: Receipt, at: { availableAt: number; now: number }) =>
      inner.release(name, receipt, at),
    ),
    count: vi.fn((name: string, now: number) => inner.count(name, now)),
    close: vi.fn(() => inner.close()),
  };
  const dialect: Dialect = {
    capabilities: { ...inner.capabilities, maxDelayMs: MAX_DELAY_MS, maxLeaseMs: MAX_LEASE_MS, ...capabilities },
    ...spies,
  };
  return { dialect, spies };
}

function clock(start: number) {
  let t = start;
  return {
    now: () => t,
    set: (next: number) => {
      t = next;
    },
  };
}

function mailQueue(dialect: Dialect, now: () => number) {
  return new Queue(dialect, { jobs: { SendMail }, now });
}

async function enqueueMail(queue: Queue<{ SendMail: typeof SendMail }>, to: string, delay?: number) {
  return queue.enqueue(new SendMail({ to }), delay === undefined ? {} : { delay });
}

describe("Queue pass-through methods", () => {
  describe("dequeue", () => {
    it("passes name, now, and leaseUntil = now + lease to the dialect", async () => {
      const { dialect, spies } = spiedDialect();
      const queue = mailQueue(dialect, () => 1_000);
      await queue.dequeue(5_000);
      expect(spies.dequeue).toHaveBeenCalledWith("default", { now: 1_000, leaseUntil: 6_000 });
    });

    it("defaults the lease to 30000 ms", async () => {
      const { dialect, spies } = spiedDialect();
      const queue = mailQueue(dialect, () => 1_000);
      await queue.dequeue();
      expect(spies.dequeue).toHaveBeenCalledWith("default", { now: 1_000, leaseUntil: 31_000 });
    });

    it("uses the queue option as the name", async () => {
      const { dialect, spies } = spiedDialect();
      const queue = new Queue(dialect, { jobs: { SendMail }, now: () => 1_000, queue: "mail" });
      await queue.dequeue();
      expect(spies.dequeue).toHaveBeenCalledWith("mail", { now: 1_000, leaseUntil: 31_000 });
    });

    it("rejects a lease over maxLeaseMs before calling the dialect", async () => {
      const { dialect, spies } = spiedDialect();
      const queue = mailQueue(dialect, () => 1_000);
      await expect(queue.dequeue(MAX_LEASE_MS + 1)).rejects.toThrow(/exceeds maxLeaseMs/);
      expect(spies.dequeue).not.toHaveBeenCalled();
    });

    it("rejects a lease too small to move leaseUntil past now before calling the dialect", async () => {
      const { dialect, spies } = spiedDialect();
      const queue = mailQueue(dialect, () => 1_700_000_000_000);
      await expect(queue.dequeue(1e-4)).rejects.toThrow(/does not move leaseUntil past now/);
      expect(spies.dequeue).not.toHaveBeenCalled();
    });

    it("rejects a zero lease before calling the dialect", async () => {
      const { dialect, spies } = spiedDialect();
      const queue = mailQueue(dialect, () => 1_000);
      await expect(queue.dequeue(0)).rejects.toThrow(/Invalid lease 0: must be a finite number of ms > 0/);
      expect(spies.dequeue).not.toHaveBeenCalled();
    });

    it.each([
      ["NaN", Number.NaN],
      ["Infinity", Number.POSITIVE_INFINITY],
      ["negative", -1],
    ])("rejects a %s lease before calling the dialect", async (_label, lease) => {
      const { dialect, spies } = spiedDialect();
      const queue = mailQueue(dialect, () => 1_000);
      await expect(queue.dequeue(lease)).rejects.toThrow(/Invalid lease/);
      expect(spies.dequeue).not.toHaveBeenCalled();
    });

    it("returns the message from MemoryDialect", async () => {
      const { dialect } = spiedDialect();
      const queue = mailQueue(dialect, () => 1_000);
      await enqueueMail(queue, "a@example.com");
      const message = await queue.dequeue();
      expect(message).not.toBeNull();
      expect(message?.payload).toBe(JSON.stringify({ job: "SendMail", payload: { to: "a@example.com" } }));
      expect(message?.attempts).toBe(1);
    });

    it("returns null when nothing is ready", async () => {
      const { dialect } = spiedDialect();
      const queue = mailQueue(dialect, () => 1_000);
      await expect(queue.dequeue()).resolves.toBeNull();
    });
  });

  describe("acknowledge", () => {
    it("removes a message dequeued through the client", async () => {
      const { dialect } = spiedDialect();
      const queue = mailQueue(dialect, () => 1_000);
      await enqueueMail(queue, "a@example.com");
      const message = await queue.dequeue();
      expect(message).not.toBeNull();
      await expect(queue.acknowledge(message?.receipt as Receipt)).resolves.toBe(true);
      await expect(queue.count()).resolves.toEqual({ ready: 0, delayed: 0, reserved: 0 });
    });

    it("resolves false on a second acknowledge of the same receipt", async () => {
      const { dialect } = spiedDialect();
      const queue = mailQueue(dialect, () => 1_000);
      await enqueueMail(queue, "a@example.com");
      const receipt = (await queue.dequeue())?.receipt as Receipt;
      await queue.acknowledge(receipt);
      await expect(queue.acknowledge(receipt)).resolves.toBe(false);
    });

    it("resolves false for a stale or unknown receipt without throwing", async () => {
      const { dialect } = spiedDialect();
      const queue = mailQueue(dialect, () => 1_000);
      await expect(queue.acknowledge("not-a-real-receipt" as Receipt)).resolves.toBe(false);
    });
  });

  describe("release", () => {
    it("with delay, hides the message until now + delay", async () => {
      const c = clock(1_000);
      const { dialect } = spiedDialect();
      const queue = mailQueue(dialect, c.now);
      await enqueueMail(queue, "a@example.com");
      const receipt = (await queue.dequeue())?.receipt as Receipt;

      await expect(queue.release(receipt, { delay: 500 })).resolves.toBe(true);

      c.set(1_499);
      await expect(queue.dequeue()).resolves.toBeNull();
      c.set(1_500);
      await expect(queue.dequeue()).resolves.not.toBeNull();
    });

    it("passes availableAt = now + delay and now to the dialect", async () => {
      const { dialect, spies } = spiedDialect();
      const queue = mailQueue(dialect, () => 1_000);
      await enqueueMail(queue, "a@example.com");
      const receipt = (await queue.dequeue())?.receipt as Receipt;
      await queue.release(receipt, { delay: 500 });
      expect(spies.release).toHaveBeenCalledWith("default", receipt, { availableAt: 1_500, now: 1_000 });
    });

    it("defaults the delay to 0 when no options are given", async () => {
      const { dialect, spies } = spiedDialect();
      const queue = mailQueue(dialect, () => 1_000);
      await enqueueMail(queue, "a@example.com");
      const receipt = (await queue.dequeue())?.receipt as Receipt;
      await queue.release(receipt);
      expect(spies.release).toHaveBeenCalledWith("default", receipt, { availableAt: 1_000, now: 1_000 });
    });

    it("rejects a delay over maxDelayMs before calling the dialect", async () => {
      const { dialect, spies } = spiedDialect();
      const queue = mailQueue(dialect, () => 1_000);
      await expect(queue.release("r" as Receipt, { delay: MAX_DELAY_MS + 1 })).rejects.toThrow(/exceeds maxDelayMs/);
      expect(spies.release).not.toHaveBeenCalled();
    });

    it.each([
      ["NaN", Number.NaN],
      ["Infinity", Number.POSITIVE_INFINITY],
      ["negative", -1],
    ])("rejects a %s delay before calling the dialect", async (_label, delay) => {
      const { dialect, spies } = spiedDialect();
      const queue = mailQueue(dialect, () => 1_000);
      await expect(queue.release("r" as Receipt, { delay })).rejects.toThrow(/Invalid delay/);
      expect(spies.release).not.toHaveBeenCalled();
    });

    it("resolves false on a stale receipt", async () => {
      const { dialect } = spiedDialect();
      const queue = mailQueue(dialect, () => 1_000);
      await expect(queue.release("stale" as Receipt, { delay: 0 })).resolves.toBe(false);
    });
  });

  describe("count", () => {
    it("reports ready, delayed, and reserved using the injected clock", async () => {
      const c = clock(1_000);
      const { dialect, spies } = spiedDialect();
      const queue = mailQueue(dialect, c.now);
      await enqueueMail(queue, "ready@example.com");
      await enqueueMail(queue, "delayed@example.com", 500);
      await queue.dequeue();

      await expect(queue.count()).resolves.toEqual({ ready: 0, delayed: 1, reserved: 1 });
      expect(spies.count).toHaveBeenLastCalledWith("default", 1_000);

      c.set(1_500);
      await expect(queue.count()).resolves.toEqual({ ready: 1, delayed: 0, reserved: 1 });
      expect(spies.count).toHaveBeenLastCalledWith("default", 1_500);
    });
  });

  describe("close", () => {
    it("resolves void and passes through to the dialect", async () => {
      const { dialect, spies } = spiedDialect();
      const queue = mailQueue(dialect, () => 1_000);
      await expect(queue.close()).resolves.toBeUndefined();
      expect(spies.close).toHaveBeenCalledTimes(1);
    });

    it("makes a later client call reject with the MemoryDialect closed error", async () => {
      const { dialect } = spiedDialect();
      const queue = mailQueue(dialect, () => 1_000);
      await queue.close();
      await expect(queue.dequeue()).rejects.toThrow("MemoryDialect is closed");
    });
  });

  describe("compile-time checks", () => {
    it("declares the compile-time checks (type-checked only, never invoked)", () => {
      // Nothing here runs: the function exists so tsc checks each @ts-expect-error line.
      const compileOnly = (queue: Queue<{ SendMail: typeof SendMail }>) => {
        // @ts-expect-error a plain string is not a Receipt
        void queue.acknowledge("plain-string");
        // @ts-expect-error a plain string is not a Receipt
        void queue.release("plain-string", { delay: 0 });
        expectTypeOf(queue.dequeue).returns.toEqualTypeOf<Promise<Message | null>>();
        expectTypeOf(queue.acknowledge).returns.toEqualTypeOf<Promise<boolean>>();
        expectTypeOf(queue.release).returns.toEqualTypeOf<Promise<boolean>>();
        expectTypeOf(queue.count).returns.toEqualTypeOf<Promise<QueueCount>>();
        expectTypeOf(queue.close).returns.toEqualTypeOf<Promise<void>>();
      };
      expect(compileOnly).toBeTypeOf("function");
    });
  });
});
