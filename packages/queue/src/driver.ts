/**
 * Storage contract every queue driver implements. Core never touches a backend directly.
 *
 * Times are epoch milliseconds, supplied by the core so drivers never read the clock.
 */

/** A job as the driver stores it. `payload` is the JSON string the core encoded. */
export interface RawJob {
  readonly id: string;
  readonly queue: string;
  readonly payload: string;
  /** Number of times this job has been reserved, including this reservation. */
  readonly attempts: number;
}

/** A job recorded in the failed store. Same payload as the queued job, plus why and when it failed. */
export interface RawFailedJob {
  readonly id: string;
  readonly queue: string;
  readonly payload: string;
  /** Reservations the job had when it failed. */
  readonly attempts: number;
  readonly error: string;
  /** Epoch ms when the job was recorded as failed. */
  readonly failedAt: number;
}

export interface QueueCounts {
  /** Ready to reserve now. */
  readonly pending: number;
  /** Pushed with a future availability time. */
  readonly delayed: number;
  /** Reserved and not yet expired. */
  readonly reserved: number;
}

export interface QueueDriver {
  /** Stores a job, available from `availableAt`. Returns the driver-assigned id. */
  insert(queue: string, payload: string, availableAt: number, now?: number): Promise<string>;

  /**
   * Atomically reserves the oldest ready job on `queue`, if any.
   * A reserved job stays hidden until `reservedUntil`; if it is not released or deleted by then,
   * it becomes ready again.
   */
  /** The returned `id` may be a reservation-scoped receipt handle; only this reservation may use it. */
  reserve(queue: string, now: number, reservedUntil: number): Promise<RawJob | null>;

  /**
   * Removes a job if the caller still holds its reservation: `attempts` matches the stored
   * attempt count and the reservation has not expired at `now`. Returns false otherwise, so a
   * stale worker cannot remove a job another worker reserved.
   */
  remove(queue: string, id: string, attempts: number, now: number): Promise<boolean>;

  /**
   * Makes a job ready again at `availableAt`, under the same reservation check as `remove`.
   * Returns false when the reservation was lost.
   */
  release(queue: string, id: string, attempts: number, now: number, availableAt: number): Promise<boolean>;

  /**
   * Removes a held job and records it in the failed store with `error`, under the same reservation
   * check. Returns false when the reservation was lost, so a stale worker cannot record a failure.
   */
  fail(queue: string, id: string, attempts: number, now: number, error: string): Promise<boolean>;

  /** Failed records on one queue, or on every queue when `queue` is null. */
  failedJobs(queue: string | null): Promise<RawFailedJob[]>;

  /** Moves one failed job back to its queue, ready now, with its attempts reset. */
  retryFailed(id: string, now: number): Promise<boolean>;

  /** Moves every failed job on one queue (or on all queues, when null) back to its queue. Returns the count. */
  retryAllFailed(queue: string | null, now: number): Promise<number>;

  /** Permanently deletes one failed record. */
  forgetFailed(id: string): Promise<boolean>;

  /** Deletes failed records on one queue (or on all queues, when null). Returns the count. */
  flushFailed(queue: string | null): Promise<number>;

  /** Deletes failed records recorded before `cutoff` (epoch ms). Returns the count. */
  pruneFailed(cutoff: number): Promise<number>;

  /** Counts jobs on `queue` as of `now`. */
  counts(queue: string, now: number): Promise<QueueCounts>;
}
