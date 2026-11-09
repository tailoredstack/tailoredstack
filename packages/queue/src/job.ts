/**
 * Base class for a queued job. One subclass per job; `payload` is the typed input.
 * `handle` runs the work and must honor `signal` for cancellation.
 */
export abstract class Job<Payload> {
  constructor(readonly payload: Payload) {}

  abstract handle(signal: AbortSignal): Promise<void>;
}
