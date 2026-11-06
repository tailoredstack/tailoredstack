import { randomUUID } from "node:crypto";
import type { QueueCounts, QueueDriver } from "./driver.ts";
import { checkJobOptions, isExhausted, type JobOptions } from "./options.ts";
import { ReservedJob } from "./reserved-job.ts";
import { checkDuration, checkQueueName } from "./validate.ts";

/** A job to push: a registered name, JSON-serializable data, and optional limits. */
export interface JobPayload {
  readonly name: string;
  readonly data?: unknown;
  readonly options?: JobOptions;
}

export interface LaterOptions {
  readonly queue?: string;
}

export interface QueueOptions {
  readonly driver: QueueDriver;
  /** Queue used by `push` and `pop` when none is named. Defaults to "default". */
  readonly defaultQueue?: string;
  /**
   * Milliseconds a reserved job stays hidden before it is ready again. Defaults to 90_000
   * (Laravel's Redis default). Must exceed the longest job runtime, or jobs run twice.
   */
  readonly retryAfter?: number;
  /** Clock in epoch milliseconds. Defaults to Date.now. */
  readonly now?: () => number;
}

const DEFAULT_QUEUE = "default";
const DEFAULT_RETRY_AFTER_MS = 90_000;

interface StoredPayload {
  readonly uuid: string;
  readonly name: string;
  readonly data: unknown;
  readonly options?: JobOptions;
}

export class Queue {
  readonly #driver: QueueDriver;
  readonly #defaultQueue: string;
  readonly #retryAfter: number;
  readonly #now: () => number;

  constructor(options: QueueOptions) {
    this.#driver = options.driver;
    this.#defaultQueue = checkQueueName(options.defaultQueue ?? DEFAULT_QUEUE);
    this.#retryAfter = checkDuration(options.retryAfter ?? DEFAULT_RETRY_AFTER_MS, "retryAfter");
    if (this.#retryAfter <= 0) throw new RangeError("retryAfter must be greater than 0");
    this.#now = options.now ?? Date.now;
  }

  /** Pushes a job onto the default queue, available now. */
  push(job: JobPayload): Promise<void> {
    return this.pushOn(this.#defaultQueue, job);
  }

  /** Pushes a job onto a named queue, available now. */
  async pushOn(queue: string, job: JobPayload): Promise<void> {
    await this.#driver.insert(checkQueueName(queue), encode(job, this.#retryAfter), this.#now());
  }

  /** Pushes a job that becomes available after `delay` milliseconds. */
  async later(delay: number, job: JobPayload, options: LaterOptions = {}): Promise<void> {
    const queue = checkQueueName(options.queue ?? this.#defaultQueue);
    const availableAt = this.#now() + checkDuration(delay, "delay");
    await this.#driver.insert(queue, encode(job, this.#retryAfter), availableAt);
  }

  /**
   * Reserves the next ready job on a queue, or returns null when none is ready. A job already past
   * its tries or retryUntil is dropped rather than returned.
   */
  async pop(queue: string = this.#defaultQueue): Promise<ReservedJob | null> {
    const name = checkQueueName(queue);
    for (;;) {
      const now = this.#now();
      const raw = await this.#driver.reserve(name, now, now + this.#retryAfter);
      if (raw === null) return null;

      const stored = JSON.parse(raw.payload) as StoredPayload;
      const options = stored.options ?? {};
      if (!isExhausted(options, raw.attempts, this.#now())) {
        return new ReservedJob(
          {
            id: raw.id,
            uuid: stored.uuid,
            queue: raw.queue,
            name: stored.name,
            data: stored.data,
            attempts: raw.attempts,
            options,
          },
          this.#driver,
          this.#now,
        );
      }
      // This reservation is past the job's limits. We hold it, so we can drop the job now.
      await this.#driver.remove(name, raw.id, raw.attempts, this.#now());
    }
  }

  /** Total jobs on a queue: pending, delayed, and reserved. */
  async size(queue: string = this.#defaultQueue): Promise<number> {
    const counts = await this.counts(queue);
    return counts.pending + counts.delayed + counts.reserved;
  }

  pendingSize(queue: string = this.#defaultQueue): Promise<number> {
    return this.counts(queue).then((c) => c.pending);
  }

  delayedSize(queue: string = this.#defaultQueue): Promise<number> {
    return this.counts(queue).then((c) => c.delayed);
  }

  reservedSize(queue: string = this.#defaultQueue): Promise<number> {
    return this.counts(queue).then((c) => c.reserved);
  }

  private counts(queue: string): Promise<QueueCounts> {
    return this.#driver.counts(checkQueueName(queue), this.#now());
  }
}

function encode(job: JobPayload, retryAfter: number): string {
  if (typeof job.name !== "string" || job.name.length === 0) {
    throw new TypeError("job name must be a non-empty string");
  }
  // JSON writes NaN and Infinity as null, so a handler would see a value it never pushed. The replacer
  // runs at every depth on the post-toJSON value, so nested numbers are checked too.
  const data = JSON.stringify(job.data ?? null, function (key, value: unknown) {
    // toJSON has already run, so an invalid Date is already null here. The holder still has the original.
    const raw: unknown = this[key];
    if (raw instanceof Date && Number.isNaN(raw.getTime())) {
      throw new TypeError(`job "${job.name}" data contains an invalid Date`);
    }
    // A custom toJSON can return a Date. The replacer sees the Date itself, which JSON would store as {}.
    if (value instanceof Date) {
      if (Number.isNaN(value.getTime())) throw new TypeError(`job "${job.name}" data contains an invalid Date`);
      return value.toISOString();
    }
    if (typeof value === "number" && !Number.isFinite(value)) {
      throw new TypeError(`job "${job.name}" data contains a non-finite number (${value})`);
    }
    return value;
  });
  // JSON.stringify(fn) is undefined and would otherwise be dropped silently. Reject it instead.
  if (data === undefined) throw new TypeError(`job "${job.name}" data is not JSON-serializable`);
  const options = checkJobOptions(job.options ?? {}, retryAfter);
  // The uuid is fixed at push, so every reservation of the job reports the same one.
  return `{"uuid":${JSON.stringify(randomUUID())},"name":${JSON.stringify(job.name)},"data":${data},"options":${JSON.stringify(options)}}`;
}
