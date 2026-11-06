import type { QueueDriver } from "./driver.ts";
import { backoffFor, type JobOptions } from "./options.ts";
import { checkDuration } from "./validate.ts";

/** Thrown when this worker no longer holds the job: the reservation expired or another worker took it. */
export class ReservationLostError extends Error {
  constructor(id: string) {
    super(`reservation for job ${id} expired or was taken by another worker`);
    this.name = "ReservationLostError";
  }
}

export interface ReservedJobInit {
  readonly id: string;
  readonly uuid: string;
  readonly queue: string;
  readonly name: string;
  readonly data: unknown;
  readonly attempts: number;
  readonly options: JobOptions;
  /** Epoch time when this worker's lease ends, on the queue's clock. */
  readonly reservedUntil: number;
}

type Settled = "deleted" | "released" | "failed";

/** A job reserved by `pop`. Settle it once with delete, release, or fail. */
export class ReservedJob {
  readonly id: string;
  readonly uuid: string;
  readonly queue: string;
  readonly name: string;
  readonly data: unknown;

  readonly #attempts: number;
  readonly #options: JobOptions;
  readonly #reservedUntil: number;
  readonly #driver: QueueDriver;
  readonly #now: () => number;
  #settled: Settled | null = null;

  constructor(init: ReservedJobInit, driver: QueueDriver, now: () => number) {
    this.id = init.id;
    this.uuid = init.uuid;
    this.queue = init.queue;
    this.name = init.name;
    this.data = init.data;
    this.#attempts = init.attempts;
    this.#options = init.options;
    this.#reservedUntil = init.reservedUntil;
    this.#driver = driver;
    this.#now = now;
  }

  /** Reservations of this job, counting this one. */
  attempts(): number {
    return this.#attempts;
  }

  /** Milliseconds left on this worker's lease. Zero or less once it has expired. */
  leaseRemaining(): number {
    return this.#reservedUntil - this.#now();
  }

  payload(): { uuid: string; name: string; data: unknown } {
    return { uuid: this.uuid, name: this.name, data: this.data };
  }

  maxTries(): number | null {
    return this.#options.tries ?? null;
  }

  timeout(): number | null {
    return this.#options.timeout ?? null;
  }

  retryUntil(): number | null {
    return this.#options.retryUntil ?? null;
  }

  failOnTimeout(): boolean {
    return this.#options.failOnTimeout ?? false;
  }

  isDeleted(): boolean {
    return this.#settled === "deleted";
  }

  isReleased(): boolean {
    return this.#settled === "released";
  }

  hasFailed(): boolean {
    return this.#settled === "failed";
  }

  /** Removes the job so it is never reserved again. */
  async delete(): Promise<void> {
    this.#assertOpen();
    await this.#remove("deleted");
  }

  /**
   * Makes the job ready again after `delay` milliseconds. With no delay, waits the backoff for the
   * attempt that just failed.
   */
  async release(delay?: number): Promise<void> {
    const wait = delay ?? backoffFor(this.#options, this.#attempts);
    checkDuration(wait, "delay");
    this.#assertOpen();
    const now = this.#now();
    const held = await this.#driver.release(this.queue, this.id, this.#attempts, now, now + wait);
    if (!held) throw new ReservationLostError(this.id);
    this.#settled = "released";
  }

  /**
   * Releases the job only after its lease ends, or after its backoff if that is longer. Use this when an
   * attempt timed out: the handler may still be running, so the job must not be reserved again first.
   */
  async releaseAfterLease(): Promise<void> {
    const wait = Math.max(backoffFor(this.#options, this.#attempts), this.leaseRemaining(), 0);
    await this.release(wait);
  }

  /**
   * Removes the job and marks it failed. The error is accepted for the failed-jobs store (TS-37),
   * which records it; this step only takes the job out of the queue.
   */
  async fail(_error?: unknown): Promise<void> {
    this.#assertOpen();
    await this.#remove("failed");
  }

  async #remove(outcome: Settled): Promise<void> {
    const held = await this.#driver.remove(this.queue, this.id, this.#attempts, this.#now());
    if (!held) throw new ReservationLostError(this.id);
    this.#settled = outcome;
  }

  #assertOpen(): void {
    if (this.#settled !== null) {
      throw new Error(`job ${this.id} is already ${this.#settled}`);
    }
  }
}
