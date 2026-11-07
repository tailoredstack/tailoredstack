import { randomUUID } from "node:crypto";
import type { QueueCounts, QueueDriver } from "./driver.ts";
import { checkJobOptions, exhaustedReason, type JobOptions } from "./options.ts";
import { ReservedJob } from "./reserved-job.ts";
import { checkDuration, checkQueueName } from "./validate.ts";
import { type JobHandler, markUnregisteredHandler, UnregisteredJobError } from "./worker.ts";

/** Handlers keyed by job name, as passed to `Queue.register`. */
export type JobHandlers = Readonly<Record<string, JobHandler>>;

/**
 * A job to push: a name, JSON-serializable data, and optional limits. `Name` is the set of registered job
 * names. Left at its default (`string`), any name is accepted; a typed queue narrows it, so an unknown name
 * is a compile error.
 */
export interface JobPayload<Name extends string = string> {
  readonly name: Name;
  readonly data?: unknown;
  readonly options?: JobOptions;
}

export interface LaterOptions {
  readonly queue?: string;
}

/** A job in the failed store, with its payload decoded. */
export interface FailedJob {
  readonly id: string;
  readonly queue: string;
  readonly name: string;
  readonly data: unknown;
  readonly attempts: number;
  readonly error: string;
  readonly failedAt: number;
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

/**
 * The names a queue accepts after `register`. An untyped queue (`string`) becomes exactly the newly registered
 * names; a typed queue adds them to the names it already has.
 */
export type RegisteredNames<Current extends string, Added extends string> = string extends Current
  ? Added
  : Current | Added;

const DEFAULT_QUEUE = "default";
const DEFAULT_RETRY_AFTER_MS = 90_000;

interface StoredPayload {
  readonly uuid: string;
  readonly name: string;
  readonly data: unknown;
  readonly options?: JobOptions;
}

export class Queue<Name extends string = string> {
  readonly #driver: QueueDriver;
  readonly #defaultQueue: string;
  readonly #retryAfter: number;
  readonly #now: () => number;
  readonly #handlers = new Map<string, JobHandler>();

  constructor(options: QueueOptions) {
    this.#driver = options.driver;
    this.#defaultQueue = checkQueueName(options.defaultQueue ?? DEFAULT_QUEUE);
    this.#retryAfter = checkDuration(options.retryAfter ?? DEFAULT_RETRY_AFTER_MS, "retryAfter");
    if (this.#retryAfter <= 0) throw new RangeError("retryAfter must be greater than 0");
    const maxVisibilityMs = this.#driver.maxVisibilityMs;
    if (maxVisibilityMs !== undefined && this.#retryAfter > maxVisibilityMs) {
      throw new RangeError(`retryAfter exceeds driver visibility maximum of ${maxVisibilityMs}ms`);
    }
    this.#now = options.now ?? Date.now;
  }

  /**
   * Registers a handler for each job name, e.g. `register({ SendInvoice: handler })`. A name can be
   * registered once. Payloads are JSON, so the name is the only link from a stored job to its code.
   *
   * Returns this queue typed by the registered names, so `push` rejects any other name at compile time.
   * Chained calls add their names to the set.
   */
  register<H extends JobHandlers>(handlers: H): Queue<RegisteredNames<Name, Extract<keyof H, string>>> {
    for (const [name, handler] of Object.entries(handlers)) {
      if (this.#handlers.has(name)) throw new Error(`job "${name}" is already registered`);
      if (typeof handler !== "function") throw new TypeError(`handler for "${name}" must be a function`);
      this.#handlers.set(name, handler);
    }
    return this as unknown as Queue<RegisteredNames<Name, Extract<keyof H, string>>>;
  }

  /**
   * The handler for a job name, for `runJob`. An unregistered name gets a handler that throws
   * UnregisteredJobError, which runJob turns into a failed job on the first attempt.
   */
  handlerFor(name: string): JobHandler {
    const handler = this.#handlers.get(name);
    if (handler) return handler;
    return markUnregisteredHandler(async () => {
      throw new UnregisteredJobError(name);
    });
  }

  /** Pushes a job onto the default queue, available now. */
  push(job: JobPayload<Name>): Promise<void> {
    return this.pushOn(this.#defaultQueue, job);
  }

  /** Pushes a job onto a named queue, available now. */
  async pushOn(queue: string, job: JobPayload<Name>): Promise<void> {
    const now = this.#now();
    await this.#driver.insert(checkQueueName(queue), encode(job, this.#retryAfter, this.#driver.maxDelayMs), now, now);
  }

  /** Pushes a job that becomes available after `delay` milliseconds. */
  async later(delay: number, job: JobPayload<Name>, options: LaterOptions = {}): Promise<void> {
    const queue = checkQueueName(options.queue ?? this.#defaultQueue);
    const now = this.#now();
    const availableAt = now + checkDuration(delay, "delay");
    await this.#driver.insert(queue, encode(job, this.#retryAfter, this.#driver.maxDelayMs), availableAt, now);
  }

  /**
   * Reserves the next ready job on a queue, or returns null when none is ready. A job already past
   * its tries or retryUntil is recorded as failed (with the reason) and skipped.
   */
  async pop(queue: string = this.#defaultQueue): Promise<ReservedJob | null> {
    const name = checkQueueName(queue);
    for (;;) {
      const now = this.#now();
      const raw = await this.#driver.reserve(name, now, now + this.#retryAfter);
      if (raw === null) return null;

      const stored = JSON.parse(raw.payload) as StoredPayload;
      const options = stored.options ?? {};
      const reason = exhaustedReason(options, raw.attempts, this.#now());
      if (reason === null) {
        return new ReservedJob(
          {
            id: raw.id,
            uuid: stored.uuid,
            queue: raw.queue,
            name: stored.name,
            data: stored.data,
            attempts: raw.attempts,
            options,
            reservedUntil: now + this.#retryAfter,
          },
          this.#driver,
          this.#now,
        );
      }
      // Past the job's limits: we hold this reservation, so record the failure with its reason (TS-37).
      await this.#driver.fail(name, raw.id, raw.attempts, this.#now(), `exhausted: ${reason}`);
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

  /** Failed jobs on one queue, or on every queue when none is named. Oldest failure first. */
  async failed(queue?: string): Promise<FailedJob[]> {
    const rows = await this.#driver.failedJobs(queue === undefined ? null : checkQueueName(queue));
    return rows.map((row) => {
      const stored = JSON.parse(row.payload) as StoredPayload;
      return {
        id: row.id,
        queue: row.queue,
        name: stored.name,
        data: stored.data,
        attempts: row.attempts,
        error: row.error,
        failedAt: row.failedAt,
      };
    });
  }

  /** Puts one failed job back on its queue, ready now, with its attempts reset. False when the id is unknown. */
  retryFailed(id: string): Promise<boolean> {
    return this.#driver.retryFailed(id, this.#now());
  }

  /** Retries every failed job on one queue, or on all queues when none is named. Returns the count. */
  retryAllFailed(queue?: string): Promise<number> {
    return this.#driver.retryAllFailed(queue === undefined ? null : checkQueueName(queue), this.#now());
  }

  /** Permanently deletes one failed record. False when the id is unknown. */
  forgetFailed(id: string): Promise<boolean> {
    return this.#driver.forgetFailed(id);
  }

  /** Deletes failed records on one queue, or on all queues when none is named. Returns the count. */
  flushFailed(queue?: string): Promise<number> {
    return this.#driver.flushFailed(queue === undefined ? null : checkQueueName(queue));
  }

  /** Deletes failed records that are older than `olderThanMs`. Returns the count. */
  async pruneFailed(olderThanMs: number): Promise<number> {
    checkDuration(olderThanMs, "olderThanMs");
    return this.#driver.pruneFailed(this.#now() - olderThanMs);
  }

  private counts(queue: string): Promise<QueueCounts> {
    return this.#driver.counts(checkQueueName(queue), this.#now());
  }
}

function encode(job: JobPayload, retryAfter: number, maxDelayMs?: number): string {
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
  const options = checkJobOptions(job.options ?? {}, retryAfter, maxDelayMs);
  // The uuid is fixed at push, so every reservation of the job reports the same one.
  return `{"uuid":${JSON.stringify(randomUUID())},"name":${JSON.stringify(job.name)},"data":${data},"options":${JSON.stringify(options)}}`;
}
