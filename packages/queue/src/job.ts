export type JobContext<Ctx> = Ctx & { readonly signal: AbortSignal };

export abstract class Job<Payload, Ctx = unknown> {
  constructor(readonly payload: Payload) {}

  abstract handle(ctx: JobContext<Ctx>): Promise<void>;
}
