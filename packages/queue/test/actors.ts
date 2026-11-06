import type { JobHandler } from "../src/index.ts";

/** One handler invocation: the job's name and data, as the handler saw them. */
export interface ActorCall {
  readonly name: string;
  readonly data: unknown;
}

/** A named test handler plus the calls it has received. */
export interface Actor {
  readonly handler: JobHandler;
  readonly calls: ActorCall[];
}

export const BAD_MESSAGE = "bad actor failed";
export const IDIOT_REASON = "idiot actor gave up";

/** Resolves normally. */
export function good(): Actor {
  const calls: ActorCall[] = [];
  return {
    calls,
    handler: async (job) => {
      calls.push({ name: job.name, data: job.data });
    },
  };
}

/** Throws an Error with a fixed message. */
export function bad(): Actor {
  const calls: ActorCall[] = [];
  return {
    calls,
    handler: async (job) => {
      calls.push({ name: job.name, data: job.data });
      throw new Error(BAD_MESSAGE);
    },
  };
}

/**
 * Ignores the AbortSignal, so it never stops early at a timeout or stop. Waits on `gate` when given,
 * then throws a plain string, which is not an Error.
 */
export function idiot(gate?: Promise<void>): Actor {
  const calls: ActorCall[] = [];
  return {
    calls,
    handler: async (job) => {
      calls.push({ name: job.name, data: job.data });
      if (gate) await gate;
      throw IDIOT_REASON;
    },
  };
}
