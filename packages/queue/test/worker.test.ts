import { describe, expect, it } from "vitest";
import { Queue, type ReservedJob, runJob } from "../src/index.ts";
import { FakeDriver } from "./fake-driver.fake.ts";

function setup() {
  let now = 1_000;
  const driver = new FakeDriver();
  const queue = new Queue({ driver, now: () => now });
  return {
    queue,
    advance: (ms: number) => {
      now += ms;
    },
  };
}

async function reserve(queue: Queue): Promise<ReservedJob> {
  const job = await queue.pop();
  if (!job) throw new Error("expected a reserved job");
  return job;
}

const neverSettles = (): Promise<void> => new Promise<void>(() => {});

describe("runJob outcomes", () => {
  it("deletes the job and reports completed when the handler finishes within its timeout", async () => {
    const { queue } = setup();
    await queue.push({ name: "fast", options: { timeout: 1_000 } });
    const job = await reserve(queue);

    const outcome = await runJob(job, async () => {});

    expect(outcome).toBe("completed");
    expect(job.isDeleted()).toBe(true);
    expect(await queue.size()).toBe(0);
  });

  it("passes the job to the handler", async () => {
    const { queue } = setup();
    await queue.push({ name: "Charge", data: { cents: 500 } });
    const job = await reserve(queue);

    let received: ReservedJob | undefined;
    await runJob(job, async (j) => {
      received = j;
    });
    expect(received).toBe(job);
  });

  it("does not abort the signal when the handler finishes first", async () => {
    const { queue } = setup();
    await queue.push({ name: "fast", options: { timeout: 1_000 } });
    const job = await reserve(queue);

    let signal: AbortSignal | undefined;
    await runJob(job, async (_job, s) => {
      signal = s;
    });
    expect(signal?.aborted).toBe(false);
  });
});

describe("timeout enforcement", () => {
  it("aborts the signal and releases the job when the handler exceeds its timeout", async () => {
    const { queue, advance } = setup();
    await queue.push({ name: "slow", options: { timeout: 20 } });
    const job = await reserve(queue);

    let signal: AbortSignal | undefined;
    const outcome = await runJob(job, (_job, s) => {
      signal = s;
      return neverSettles();
    });

    expect(outcome).toBe("timed-out");
    expect(signal?.aborted).toBe(true);
    expect(job.isReleased()).toBe(true);
    advance(90_000); // default retryAfter: the lease ends here
    expect((await reserve(queue)).attempts()).toBe(2);
  });

  it("returns at the timeout even when the handler ignores the signal", async () => {
    const { queue } = setup();
    await queue.push({ name: "stuck", options: { timeout: 20 } });
    const job = await reserve(queue);

    const started = Date.now();
    await runJob(job, neverSettles);
    expect(Date.now() - started).toBeLessThan(1_000);
  });

  it("fails the job instead of retrying it when failOnTimeout is set", async () => {
    const { queue } = setup();
    await queue.push({ name: "strict", options: { timeout: 20, failOnTimeout: true } });
    const job = await reserve(queue);

    const outcome = await runJob(job, neverSettles);

    expect(outcome).toBe("timed-out");
    expect(job.hasFailed()).toBe(true);
    expect(job.isReleased()).toBe(false);
    expect(await queue.size()).toBe(0);
  });

  it("does not time out a job with no timeout option", async () => {
    const { queue } = setup();
    await queue.push({ name: "untimed" });
    const job = await reserve(queue);

    let signal: AbortSignal | undefined;
    const pending = runJob(job, (_job, s) => {
      signal = s;
      return new Promise<void>((resolve) => setTimeout(resolve, 30));
    });
    await expect(pending).resolves.toBe("completed");
    expect(signal?.aborted).toBe(false);
  });
});

describe("lease after a timed-out attempt", () => {
  it("keeps a timed-out job hidden until its reservation expires", async () => {
    const { queue, advance } = setup();
    await queue.push({ name: "stuck", options: { timeout: 20 } });
    const job = await reserve(queue);

    await runJob(job, neverSettles);
    expect(await queue.pop()).toBeNull();

    advance(89_999);
    expect(await queue.pop()).toBeNull();

    advance(1);
    expect((await reserve(queue)).attempts()).toBe(2);
  });
});

describe("handler errors", () => {
  it("releases the job with the backoff for the failed attempt when the handler throws", async () => {
    const { queue, advance } = setup();
    await queue.push({ name: "flaky", options: { tries: 3, backoff: 500 } });
    const job = await reserve(queue);

    const outcome = await runJob(job, async () => {
      throw new Error("boom");
    });

    expect(outcome).toBe("errored");
    expect(job.isReleased()).toBe(true);
    expect(await queue.pop()).toBeNull();

    advance(500);
    expect((await reserve(queue)).attempts()).toBe(2);
  });

  it("treats a synchronous throw from the handler the same as a rejection", async () => {
    const { queue } = setup();
    await queue.push({ name: "sync" });
    const job = await reserve(queue);

    const outcome = await runJob(job, () => {
      throw new Error("sync boom");
    });
    expect(outcome).toBe("errored");
    expect(job.isReleased()).toBe(true);
  });
});
