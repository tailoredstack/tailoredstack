/**
 * Per-job options. They are stored with the payload, so every reservation of a job sees the same limits.
 * Times are milliseconds, except retryUntil, which is an absolute epoch time.
 */
export interface JobOptions {
  /** Maximum reservations of the job. Undefined means no limit. */
  readonly tries?: number;
  /** Per-attempt runtime limit. Enforced by the worker (TS-32). Must be below retryAfter. */
  readonly timeout?: number;
  /** Wait before a retry: one value for every retry, or one value per retry. */
  readonly backoff?: number | readonly number[];
  /** Absolute epoch time after which the job is dropped instead of attempted. */
  readonly retryUntil?: number;
  /** When an attempt times out, fail the job instead of retrying it. Enforced by the worker (TS-32). */
  readonly failOnTimeout?: boolean;
}

/** Validates options against the queue's retryAfter. Throws before anything is stored. */
export function checkJobOptions(options: JobOptions, retryAfter: number, maxDelayMs?: number): JobOptions {
  const { tries, timeout, backoff, retryUntil, failOnTimeout } = options;

  if (tries !== undefined && (!Number.isInteger(tries) || tries < 1)) {
    throw new RangeError(`tries must be an integer >= 1, got ${tries}`);
  }

  if (timeout !== undefined) {
    if (!Number.isFinite(timeout) || timeout <= 0) {
      throw new RangeError(`timeout must be a finite number of ms > 0, got ${timeout}`);
    }
    if (timeout >= retryAfter) {
      throw new RangeError(`timeout (${timeout}) must be less than retryAfter (${retryAfter}), or the job runs twice`);
    }
  }

  if (backoff !== undefined) {
    if (typeof backoff === "number") {
      checkBackoffValue(backoff);
      checkMaxBackoff(backoff, maxDelayMs);
    } else {
      // An array is one entry per retry, so its length can only be checked when tries is known.
      if (tries === undefined) throw new RangeError("a backoff array needs tries, so its length can be checked");
      if (backoff.length < tries - 1) {
        throw new RangeError(`backoff needs at least ${tries - 1} entries for tries ${tries}, got ${backoff.length}`);
      }
      backoff.forEach((value) => {
        checkBackoffValue(value);
        checkMaxBackoff(value, maxDelayMs);
      });
    }
  }

  if (retryUntil !== undefined && !Number.isFinite(retryUntil)) {
    throw new RangeError(`retryUntil must be a finite epoch time, got ${retryUntil}`);
  }

  if (failOnTimeout !== undefined && typeof failOnTimeout !== "boolean") {
    throw new TypeError("failOnTimeout must be a boolean");
  }

  return options;
}

/**
 * Why a reservation at `now` is past the job's limits: "tries" or "retryUntil". Null when the job may run.
 * The job is recorded as failed with this reason, not run and not dropped silently.
 */
export function exhaustedReason(options: JobOptions, attempts: number, now: number): "tries" | "retryUntil" | null {
  if (options.tries !== undefined && attempts > options.tries) return "tries";
  if (options.retryUntil !== undefined && now >= options.retryUntil) return "retryUntil";
  return null;
}

/** Wait before the next attempt, for the attempt that just failed. Zero when no backoff applies. */
export function backoffFor(options: JobOptions, attempts: number): number {
  const backoff = options.backoff;
  if (backoff === undefined) return 0;
  if (typeof backoff === "number") return backoff;
  // Entry i is the wait after attempt i + 1. The last attempt has no retry, so it has no entry.
  return backoff[attempts - 1] ?? 0;
}

function checkMaxBackoff(value: number, maxDelayMs: number | undefined): void {
  if (maxDelayMs !== undefined && value > maxDelayMs) {
    throw new RangeError(`backoff (${value}ms) exceeds driver maximum (${maxDelayMs}ms)`);
  }
}

function checkBackoffValue(value: number): void {
  if (!Number.isFinite(value) || value < 0) {
    throw new RangeError(`backoff must be a finite number of ms >= 0, got ${value}`);
  }
}
