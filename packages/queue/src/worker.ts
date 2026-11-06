import type { ReservedJob } from "./reserved-job.ts";

/**
 * How one attempt ended:
 * - completed: the handler finished; the job was deleted.
 * - errored: the handler threw or rejected; the job was released with its backoff.
 * - timed-out: the attempt ran past its timeout; the job was released, or failed when failOnTimeout is set.
 */
export type JobOutcome = "completed" | "errored" | "timed-out";

/**
 * Work for one job. The signal aborts when the attempt times out. A handler that ignores the signal
 * keeps running, but the worker stops waiting for it at the timeout (Node cannot kill a promise).
 */
export type JobHandler = (job: ReservedJob, signal: AbortSignal) => unknown;

/**
 * Runs one reserved job and settles it. Each job is settled exactly once, here, so callers do not
 * call delete, release, or fail themselves.
 *
 * The timeout uses the real clock (setTimeout), not the queue's injected clock. retryAfter must stay
 * above timeout so the reservation does not expire while the handler runs.
 */
export async function runJob(job: ReservedJob, handler: JobHandler): Promise<JobOutcome> {
  const controller = new AbortController();
  const limit = job.timeout();

  // Never rejects: a handler that throws, including synchronously, resolves to "errored".
  const attempt = new Promise<"completed" | "errored">((resolve) => {
    new Promise<unknown>((settle) => settle(handler(job, controller.signal))).then(
      () => resolve("completed"),
      () => resolve("errored"),
    );
  });

  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<"timed-out">((resolve) => {
    if (limit === null) return;
    timer = setTimeout(() => {
      controller.abort();
      resolve("timed-out");
    }, limit);
  });

  const result = await Promise.race([attempt, deadline]);
  clearTimeout(timer);

  if (result === "completed") {
    await job.delete();
    return "completed";
  }
  if (result === "timed-out" && job.failOnTimeout()) {
    await job.fail(new Error(`job ${job.id} timed out after ${limit} ms`));
    return "timed-out";
  }
  await job.release();
  return result === "timed-out" ? "timed-out" : "errored";
}
