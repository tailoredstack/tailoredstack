import { it } from "@effect/vitest";
import { Effect, Exit } from "effect";
import { describe, expect } from "vitest";
import type { Dialect, DialectCapabilities } from "../dialect.ts";
import { InvalidArgumentError, LimitError, QueueClosedError } from "../errors.ts";
import { Job, type JobContext } from "../job.ts";
import { MemoryDialect } from "../memory-dialect.ts";
import { Queue } from "../queue.ts";
import { EffectQueue, QueueError } from "./index.ts";

class SendMail extends Job<{ readonly to: string }> {
  declare readonly __jobKey: "SendMail";

  async handle(_ctx: JobContext<unknown>): Promise<void> {}
}

const make = () =>
  new EffectQueue(
    new Queue(new MemoryDialect(), {
      jobs: { SendMail },
      now: () => 1_000,
    }),
  );

const boom = new Error("broker down");

const capabilities: DialectCapabilities = {
  ordered: true,
  exactCounts: true,
  clockSource: "caller",
  maxDelayMs: 60_000,
  maxLeaseMs: 60_000,
};

const failingDialect: Dialect = {
  capabilities,
  enqueue: async () => {
    throw boom;
  },
  dequeue: async () => {
    throw boom;
  },
  acknowledge: async () => {
    throw boom;
  },
  release: async () => {
    throw boom;
  },
  count: async () => {
    throw boom;
  },
  close: async () => {
    throw boom;
  },
};

const failingQueue = () => new EffectQueue(new Queue(failingDialect, { jobs: { SendMail } }));

describe("EffectQueue", () => {
  it.effect("enqueue returns the message id", () =>
    Effect.gen(function* () {
      expect(yield* make().enqueue(new SendMail({ to: "ada@example.com" }))).toBeTypeOf("string");
    }),
  );

  it.effect("dequeue yields null until the delay passes, then the leased message", () =>
    Effect.gen(function* () {
      let now = 1_000;
      const queue = new EffectQueue(
        new Queue(new MemoryDialect(), {
          jobs: { SendMail },
          now: () => now,
        }),
      );
      yield* queue.enqueue(new SendMail({ to: "ada@example.com" }), { delay: 500 });
      expect(yield* queue.dequeue(1_000)).toBeNull();
      now = 1_500;
      const message = yield* queue.dequeue(1_000);
      expect(message).not.toBeNull();
      expect(message?.payload).toBe(JSON.stringify({ job: "SendMail", payload: { to: "ada@example.com" } }));
    }),
  );

  it.effect("acknowledge settles the reservation and count reflects it", () =>
    Effect.gen(function* () {
      const queue = make();
      yield* queue.enqueue(new SendMail({ to: "ada@example.com" }));
      const message = yield* queue.dequeue(1_000);
      if (message === null) throw new Error("expected a message");
      const acknowledged = yield* queue.acknowledge(message.receipt);
      expect({ acknowledged, count: yield* queue.count() }).toEqual({
        acknowledged: true,
        count: { ready: 0, delayed: 0, reserved: 0 },
      });
    }),
  );

  it.effect("release puts the reservation back and count sees it as ready", () =>
    Effect.gen(function* () {
      const queue = make();
      yield* queue.enqueue(new SendMail({ to: "ada@example.com" }));
      const message = yield* queue.dequeue(1_000);
      if (message === null) throw new Error("expected a message");
      const released = yield* queue.release(message.receipt);
      expect({ released, count: yield* queue.count() }).toEqual({
        released: true,
        count: { ready: 1, delayed: 0, reserved: 0 },
      });
    }),
  );

  it.effect("close completes", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(make().close());
      expect(Exit.isSuccess(exit)).toBe(true);
    }),
  );
});

describe("EffectQueue error passthrough", () => {
  it.effect("dialect failures become QueueError with the operation and the original cause", () =>
    Effect.gen(function* () {
      const queue = failingQueue();
      const calls = [
        { operation: "enqueue", effect: queue.enqueue(new SendMail({ to: "ada@example.com" })) },
        { operation: "dequeue", effect: queue.dequeue(1_000) },
        { operation: "acknowledge", effect: queue.acknowledge("receipt" as never) },
        { operation: "release", effect: queue.release("receipt" as never) },
        { operation: "count", effect: queue.count() },
        { operation: "close", effect: queue.close() },
      ];

      for (const { operation, effect } of calls) {
        const error = yield* Effect.flip(effect);
        expect(error).toBeInstanceOf(QueueError);
        expect((error as QueueError)._tag).toBe("QueueError");
        expect((error as QueueError).operation).toBe(operation);
        expect((error as QueueError).cause).toBe(boom);
      }
    }),
  );

  it.effect("invalid arguments pass through as InvalidArgumentError", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(make().enqueue(new SendMail({ to: "ada@example.com" }), { delay: -1 }));
      expect(error).toBeInstanceOf(InvalidArgumentError);
      expect(error).not.toBeInstanceOf(QueueError);
      expect((error as Error).message).toMatch(/Invalid delay/);
    }),
  );

  it.effect("limit violations pass through as LimitError", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(make().enqueue(new SendMail({ to: "a".repeat(262_144) })));
      expect(error).toBeInstanceOf(LimitError);
      expect(error).not.toBeInstanceOf(QueueError);
      expect((error as Error).message).toMatch(/over the 262144 byte limit/);
    }),
  );

  it.effect("calls after close pass through as QueueClosedError", () =>
    Effect.gen(function* () {
      const queue = make();
      yield* queue.close();
      const error = yield* Effect.flip(queue.dequeue());
      expect(error).toBeInstanceOf(QueueClosedError);
      expect(error).not.toBeInstanceOf(QueueError);
      expect((error as Error).message).toBe("MemoryDialect is closed");
    }),
  );
});
