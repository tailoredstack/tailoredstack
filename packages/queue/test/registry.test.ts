import { describe, expect, it } from "vitest";
import { Queue, type ReservedJob, runJob } from "../src/index.ts";
import { FakeDriver } from "./fake-driver.fake.ts";

function setup() {
  let now = 1_000;
  const driver = new FakeDriver();
  const queue = new Queue({ driver, now: () => now });
  return { queue, driver };
}

async function reserve(queue: Queue): Promise<ReservedJob> {
  const job = await queue.pop();
  if (!job) throw new Error("expected a reserved job");
  return job;
}

const signal = () => new AbortController().signal;

describe("job registry", () => {
  it("runs the handler registered under the job's name and deletes the job", async () => {
    const { queue } = setup();
    let received: unknown;
    queue.register({
      Ping: async () => {},
      SendInvoice: async (j) => {
        received = j.data;
      },
    });
    await queue.push({ name: "SendInvoice", data: { id: 7 } });
    const job = await reserve(queue);

    const outcome = await runJob(job, queue.handlerFor(job.name));

    expect(outcome).toBe("completed");
    expect(received).toEqual({ id: 7 });
    expect(job.isDeleted()).toBe(true);
  });

  it("fails the attempt with an error that names the job when no handler is registered", async () => {
    const { queue } = setup();
    queue.register({ SendInvoice: async () => {} });
    await queue.push({ name: "CancelInvoice" });
    const job = await reserve(queue);

    await expect(queue.handlerFor(job.name)(job, signal())).rejects.toThrow(/CancelInvoice/);
  });

  it("releases an unregistered job for retry instead of completing or dropping it", async () => {
    const { queue } = setup();
    await queue.push({ name: "CancelInvoice", options: { tries: 3 } });
    const job = await reserve(queue);

    const outcome = await runJob(job, queue.handlerFor(job.name));

    expect(outcome).toBe("errored");
    expect(job.isDeleted()).toBe(false);
    expect(job.isReleased()).toBe(true);
    expect(await queue.size()).toBe(1);
  });

  it("rejects a second registration of the same name", () => {
    const { queue } = setup();
    queue.register({ SendInvoice: async () => {} });
    expect(() => queue.register({ SendInvoice: async () => {} })).toThrow(/SendInvoice/);
  });

  it("rejects a value that is not a function", () => {
    const { queue } = setup();
    expect(() => queue.register({ SendInvoice: "not a handler" as unknown as () => void })).toThrow(/handler for "SendInvoice" must be a function/);
  });
});
