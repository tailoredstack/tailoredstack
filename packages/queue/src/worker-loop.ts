import type { Queue } from "./queue.ts";
import { isUnregisteredHandler, type JobHandler, runJob } from "./worker.ts";

export interface WorkerLoopOptions {
  /** Queue to pop from. */
  readonly queue: Queue<string>;
  /** Named queue to pop from. Defaults to the queue's default. */
  readonly queueName?: string;
  /** Stop request. Once aborted, no new job is reserved; the attempt in progress finishes first. */
  readonly signal: AbortSignal;
  /** Milliseconds to wait before polling again after an empty pop or a driver error. */
  readonly pollInterval: number;
  /**
   * Receives each error from a handler, from runJob (including UnregisteredJobError), or from the driver's
   * pop. The loop keeps running after any of them.
   */
  readonly onError: (error: unknown) => void;
}

/**
 * Pops and runs reserved jobs one at a time until `signal` aborts. Each job is settled by runJob, so the
 * loop never deletes, releases, or fails a job itself. Resolves after the attempt in progress settles.
 */
export async function runWorkerLoop(options: WorkerLoopOptions): Promise<void> {
  const { queue, queueName, signal, pollInterval, onError } = options;

  while (!signal.aborted) {
    let job: Awaited<ReturnType<Queue<string>["pop"]>>;
    try {
      job = await queue.pop(queueName);
    } catch (error) {
      onError(error);
      await wait(pollInterval, signal);
      continue;
    }
    if (job === null) {
      await wait(pollInterval, signal);
      continue;
    }

    try {
      await runJob(job, reportingHandler(queue.handlerFor(job.name), onError));
    } catch (error) {
      onError(error);
    }
  }
}

/**
 * Passes handler errors to onError, then rethrows so runJob settles the job as errored. An unregistered
 * handler is returned as-is: wrapping it would hide the mark runJob needs to fail the job on its first try.
 */
function reportingHandler(handler: JobHandler, onError: (error: unknown) => void): JobHandler {
  if (isUnregisteredHandler(handler)) return handler;
  return async (job, signal) => {
    try {
      return await handler(job, signal);
    } catch (error) {
      onError(error);
      throw error;
    }
  };
}

/** Resolves after `ms`, or as soon as `signal` aborts. Never rejects. */
function wait(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) return resolve();
    const done = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", done);
      resolve();
    };
    const timer = setTimeout(done, ms);
    signal.addEventListener("abort", done, { once: true });
  });
}
