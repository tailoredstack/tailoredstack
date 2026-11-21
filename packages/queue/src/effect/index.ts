import { Data, Effect } from "effect";
import type { Message, QueueCount, Receipt } from "../dialect.ts";
import { InvalidArgumentError, LimitError, QueueClosedError } from "../errors.ts";
import type { EnqueueOptions, JobRegistry, Queue, ReleaseOptions } from "../queue.ts";

export class QueueError extends Data.TaggedError("QueueError")<{
  readonly operation: string;
  readonly cause: unknown;
}> {}

/** Failures the caller can act on; they pass through the wrapper unchanged. */
export type QueueDomainError = InvalidArgumentError | LimitError | QueueClosedError;

/** Effect-returning Queue. Domain errors pass through; anything else becomes a QueueError. */
export class EffectQueue<R extends JobRegistry> {
  readonly #queue: Queue<R>;

  constructor(queue: Queue<R>) {
    this.#queue = queue;
  }

  #guard<A>(operation: string, run: () => Promise<A>): Effect.Effect<A, QueueError | QueueDomainError> {
    return Effect.tryPromise({ try: run, catch: (cause) => cause }).pipe(
      Effect.catch((cause): Effect.Effect<never, QueueError | QueueDomainError> => {
        if (cause instanceof InvalidArgumentError || cause instanceof LimitError || cause instanceof QueueClosedError) {
          return Effect.fail(cause);
        }
        return Effect.fail(new QueueError({ operation, cause }));
      }),
    );
  }

  enqueue(
    job: InstanceType<R[keyof R]>,
    options?: EnqueueOptions,
  ): Effect.Effect<string, QueueError | QueueDomainError> {
    return this.#guard("enqueue", () => this.#queue.enqueue(job, options));
  }

  dequeue(lease?: number): Effect.Effect<Message | null, QueueError | QueueDomainError> {
    return this.#guard("dequeue", () => this.#queue.dequeue(lease));
  }

  acknowledge(receipt: Receipt): Effect.Effect<boolean, QueueError | QueueDomainError> {
    return this.#guard("acknowledge", () => this.#queue.acknowledge(receipt));
  }

  release(receipt: Receipt, options?: ReleaseOptions): Effect.Effect<boolean, QueueError | QueueDomainError> {
    return this.#guard("release", () => this.#queue.release(receipt, options));
  }

  count(): Effect.Effect<QueueCount, QueueError | QueueDomainError> {
    return this.#guard("count", () => this.#queue.count());
  }

  close(): Effect.Effect<void, QueueError | QueueDomainError> {
    return this.#guard("close", () => this.#queue.close());
  }
}

export { InvalidArgumentError, LimitError, QueueClosedError } from "../errors.ts";
