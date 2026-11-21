import { it } from "@effect/vitest";
import { Effect, Exit } from "effect";
import { describe, expect } from "vitest";
import { Job, type JobContext } from "../job.ts";
import { MemoryDialect } from "../memory-dialect.ts";
import { Queue } from "../queue.ts";
import { EffectQueue } from "./index.ts";

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

  it.effect("a validation failure maps to a tagged QueueError", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(make().enqueue(new SendMail({ to: "ada@example.com" }), { delay: -1 }));
      expect(Exit.isFailure(exit)).toBe(true);
      expect(String(exit)).toMatch(/QueueError/);
      expect(String(exit)).toMatch(/delay/);
    }),
  );
});
