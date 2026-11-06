import type { QueueCounts, QueueDriver, RawFailedJob, RawJob } from "./driver.ts";
import type { JobOptions } from "./options.ts";
import { ReservedJob } from "./reserved-job.ts";

/** Runs one job. Errors it throws propagate to the caller of push. */
export type SyncHandler = (job: ReservedJob) => unknown;

/**
 * Runs each job inline, inside `insert`, in the caller's process. Nothing is stored, so there is no
 * worker, no retry, and no failed store: a handler error rejects the push call, as Laravel's sync
 * driver does. `later` runs immediately too, ignoring the delay, because there is no queue to wait in.
 *
 * Decodes the same payload string the stored drivers keep, so the handler sees the same name and data.
 * Used in development and tests.
 */
export class SyncDriver implements QueueDriver {
  readonly #handler: SyncHandler;

  constructor(handler: SyncHandler) {
    this.#handler = handler;
  }

  async insert(queue: string, payload: string, availableAt: number): Promise<string> {
    const stored = JSON.parse(payload) as { uuid: string; name: string; data: unknown; options?: JobOptions };
    const job = new ReservedJob(
      {
        id: stored.uuid,
        uuid: stored.uuid,
        queue,
        name: stored.name,
        data: stored.data,
        attempts: 1,
        options: stored.options ?? {},
        reservedUntil: availableAt,
      },
      inlineLease,
      () => availableAt,
    );
    await this.#handler(job);
    return stored.uuid;
  }

  reserve(): Promise<RawJob | null> {
    return Promise.resolve(null);
  }

  remove(): Promise<boolean> {
    return Promise.resolve(true);
  }

  release(): Promise<boolean> {
    return Promise.resolve(true);
  }

  counts(): Promise<QueueCounts> {
    return Promise.resolve({ pending: 0, delayed: 0, reserved: 0 });
  }

  // An inline job is never stored, so there is no failed store to read or change.
  fail(): Promise<boolean> {
    return Promise.resolve(true);
  }

  failedJobs(): Promise<RawFailedJob[]> {
    return Promise.resolve([]);
  }

  retryFailed(): Promise<boolean> {
    return Promise.resolve(false);
  }

  retryAllFailed(): Promise<number> {
    return Promise.resolve(0);
  }

  forgetFailed(): Promise<boolean> {
    return Promise.resolve(false);
  }

  flushFailed(): Promise<number> {
    return Promise.resolve(0);
  }

  pruneFailed(): Promise<number> {
    return Promise.resolve(0);
  }
}

/** Settling an inline job has nothing to remove or release, so every settle call succeeds. */
const inlineLease: QueueDriver = {
  insert: async () => "",
  reserve: async () => null,
  remove: async () => true,
  release: async () => true,
  fail: async () => true,
  failedJobs: async () => [],
  retryFailed: async () => false,
  retryAllFailed: async () => 0,
  forgetFailed: async () => false,
  flushFailed: async () => 0,
  pruneFailed: async () => 0,
  counts: async () => ({ pending: 0, delayed: 0, reserved: 0 }),
};
