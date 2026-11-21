import { Data, Effect } from "effect";
import type { Message, QueueCount, Receipt } from "../dialect.ts";
import type { EnqueueOptions, JobRegistry, Queue, ReleaseOptions } from "../queue.ts";

export class QueueError extends Data.TaggedError("QueueError")<{
  readonly operation: string;
  readonly cause: unknown;
}> {}

/** Effect-returning Queue. Broker, validation, and serialization failures surface as QueueError. */
export class EffectQueue<R extends JobRegistry> {
  readonly #queue: Queue<R>;

  constructor(queue: Queue<R>) {
    this.#queue = queue;
  }

  #try<A>(operation: string, run: () => Promise<A>): Effect.Effect<A, QueueError> {
    return Effect.tryPromise({
      try: run,
      catch: (cause) => new QueueError({ operation, cause }),
    });
  }

  enqueue(job: InstanceType<R[keyof R]>, options?: EnqueueOptions): Effect.Effect<string, QueueError> {
    return this.#try("enqueue", () => this.#queue.enqueue(job, options));
  }

  dequeue(lease?: number): Effect.Effect<Message | null, QueueError> {
    return this.#try("dequeue", () => this.#queue.dequeue(lease));
  }

  acknowledge(receipt: Receipt): Effect.Effect<boolean, QueueError> {
    return this.#try("acknowledge", () => this.#queue.acknowledge(receipt));
  }

  release(receipt: Receipt, options?: ReleaseOptions): Effect.Effect<boolean, QueueError> {
    return this.#try("release", () => this.#queue.release(receipt, options));
  }

  count(): Effect.Effect<QueueCount, QueueError> {
    return this.#try("count", () => this.#queue.count());
  }

  close(): Effect.Effect<void, QueueError> {
    return this.#try("close", () => this.#queue.close());
  }
}
