/** Caller-supplied context merged with the per-run abort signal. */
export type JobContext<Ctx> = Ctx & { readonly signal: AbortSignal };

/**
 * Base class for a queued job. One subclass per job; `payload` is the typed input.
 * `handle` runs the work with one context argument; `ctx.signal` is the cancellation signal.
 */
export abstract class Job<Payload, Ctx = unknown> {
  constructor(readonly payload: Payload) {}

  abstract handle(ctx: JobContext<Ctx>): Promise<void>;
}
