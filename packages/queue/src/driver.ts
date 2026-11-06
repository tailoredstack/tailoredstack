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
  insert(queue: string, payload: string, availableAt: number): Promise<string>;

  /**
   * Atomically reserves the oldest ready job on `queue`, if any.
   * A reserved job stays hidden until `reservedUntil`; if it is not released or deleted by then,
   * it becomes ready again.
   */
  reserve(queue: string, now: number, reservedUntil: number): Promise<RawJob | null>;

  /** Counts jobs on `queue` as of `now`. */
  counts(queue: string, now: number): Promise<QueueCounts>;
}
