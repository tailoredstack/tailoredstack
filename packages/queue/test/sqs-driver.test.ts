import { describe, expect, it } from "vitest";
import { SqsQueueDriver } from "../src/drivers/sqs.ts";
import { Queue } from "../src/queue.ts";
import { runJob } from "../src/worker.ts";
import { FakeSqsClient } from "./fake-sqs-client.fake.ts";

const FAILED_URL = "https://sqs.test/failed";
const MAX_DELAY_MS = 900_000;
const MAX_LEASE_MS = 43_200_000;

function setup() {
  const client = new FakeSqsClient();
  const driver = new SqsQueueDriver({
    client,
    queueUrl: (queue) => `https://sqs.test/${queue}`,
    failedQueueUrl: FAILED_URL,
  });
  return { client, driver };
}

describe("SQS backoff validation", () => {
  it("rejects a retryAfter above the SQS visibility maximum during Queue construction", () => {
    const { driver } = setup();

    expect(() => new Queue({ driver, retryAfter: MAX_LEASE_MS + 1 })).toThrow(/visibility.*maximum/i);
  });

  it("rejects any backoff entry above 900s at push before storing the job", async () => {
    const { client, driver } = setup();
    const queue = new Queue({ driver, now: () => 0 });

    await expect(
      queue.push({ name: "retry", options: { tries: 3, backoff: [100, MAX_DELAY_MS + 1] } }),
    ).rejects.toThrow(RangeError);
    expect(client.calls.filter((call) => call.op === "SendMessage")).toHaveLength(0);
  });

  it("accepts a backoff exactly at 900s", async () => {
    const { client, driver } = setup();
    const queue = new Queue({ driver, now: () => 0 });

    await expect(queue.push({ name: "retry", options: { tries: 2, backoff: MAX_DELAY_MS } })).resolves.toBeUndefined();
    expect(client.calls.filter((call) => call.op === "SendMessage")).toHaveLength(1);
  });
});

describe("insert and reserve", () => {
  it("returns an inserted job on reserve with attempts 1", async () => {
    const { driver } = setup();
    await driver.insert("default", '{"n":1}', 0, 0);

    expect(await driver.reserve("default", 0, 1_000)).toMatchObject({
      id: expect.stringContaining("receipt-"),
      queue: "default",
      payload: '{"n":1}',
      attempts: 1,
    });
  });

  it("rejects a delay over 900s before sending anything", async () => {
    const { client, driver } = setup();

    await expect(driver.insert("default", "{}", MAX_DELAY_MS + 1_000, 0)).rejects.toThrow(/900/);
    expect(client.calls.filter((c) => c.op === "SendMessage")).toHaveLength(0);
  });

  it("accepts a delay of exactly 900s", async () => {
    const { client, driver } = setup();

    await driver.insert("default", "{}", MAX_DELAY_MS, 0);
    expect(client.calls.filter((c) => c.op === "SendMessage")).toHaveLength(1);
  });

  it("hides a reserved job until its lease ends, then it is ready again with attempts 2", async () => {
    const { client, driver } = setup();
    await driver.insert("default", "{}", 0, 0);

    const first = await driver.reserve("default", 0, 1_000);
    expect(first).toMatchObject({ id: expect.stringContaining("receipt-"), attempts: 1 });

    client.clock = 999;
    expect(await driver.reserve("default", 999, 2_000)).toBeNull();

    client.clock = 1_000;
    expect(await driver.reserve("default", 1_000, 2_000)).toMatchObject({
      id: expect.stringContaining("receipt-"),
      queue: "default",
      payload: "{}",
      attempts: 2,
    });
  });
});

describe("lease-based release", () => {
  it("settles timed-out jobs when the remaining lease exceeds SQS's 900-second delay limit", async () => {
    const client = new FakeSqsClient();
    const driver = new SqsQueueDriver({
      client,
      queueUrl: (queue) => `https://sqs.test/${queue}`,
      failedQueueUrl: FAILED_URL,
    });
    const queue = new Queue({ driver, retryAfter: 901_000, now: () => client.clock });
    await queue.push({ name: "slow", options: { timeout: 1 } });
    const reserved = await queue.pop();
    if (!reserved) throw new Error("expected reservation");

    await expect(runJob(reserved, () => new Promise(() => {}))).resolves.toBe("timed-out");
    client.clock = 900_000;
    expect(await queue.pop()).toBeNull();
    client.clock = 901_000;
    expect(await queue.pop()).not.toBeNull();
  });
});

describe("release delays", () => {
  it("rejects an explicit release delay above 900s instead of clamping", async () => {
    const { client, driver } = setup();
    await driver.insert("default", "delayed", 0, 0);
    const reserved = await driver.reserve("default", 0, 1_000);
    if (!reserved) throw new Error("expected reservation");

    await expect(driver.release("default", reserved.id, 1, 0, 901_000)).rejects.toThrow(/900/);
    expect(client.calls.filter((call) => call.op === "SendMessage")).toHaveLength(1);
  });
});

describe("release delivery guarantees", () => {
  it("documents at-least-once duplication if deleting after re-send fails", async () => {
    const { client, driver } = setup();
    await driver.insert("default", "work", 0, 0);
    const reserved = await driver.reserve("default", 0, 1_000);
    if (!reserved) throw new Error("expected reservation");

    client.failNextDelete = true;
    await expect(driver.release("default", reserved.id, 1, 0, 0)).rejects.toThrow("injected delete failure");
    expect(await driver.reserve("default", 0, 1_000)).toMatchObject({ payload: "work" });
    client.clock = 1_000;
    expect(await driver.reserve("default", 1_000, 2_000)).toMatchObject({ payload: "work" });
  });
});

describe("stale attempts", () => {
  it("remove, release and fail return false for an attempt that no longer holds the job", async () => {
    const { client, driver } = setup();
    await driver.insert("default", "{}", 0, 0);
    const first = await driver.reserve("default", 0, 1_000);
    if (!first) throw new Error("expected first reservation");

    client.clock = 1_000;
    await driver.reserve("default", 1_000, 2_000); // attempt 2 now holds the job

    expect(await driver.remove("default", first.id, 1, 1_500)).toBe(false);
    expect(await driver.release("default", first.id, 1, 1_500, 0)).toBe(false);
    expect(await driver.fail("default", first.id, 1, 1_500, "stale")).toBe(false);

    expect(client.calls.filter((c) => c.op === "DeleteMessage")).toHaveLength(0);
    expect(client.calls.filter((c) => c.op === "SendMessage" && c.input.QueueUrl === FAILED_URL)).toHaveLength(0);
  });

  it("remove returns false once the lease has expired, even for the current attempt", async () => {
    const { driver } = setup();
    await driver.insert("default", "{}", 0, 0);
    const reserved = await driver.reserve("default", 0, 1_000);
    if (!reserved) throw new Error("expected reservation");

    expect(await driver.remove("default", reserved.id, 1, 1_000)).toBe(false);
  });

  it("the current holder can remove the job", async () => {
    const { driver } = setup();
    await driver.insert("default", "{}", 0, 0);
    const reserved = await driver.reserve("default", 0, 1_000);
    if (!reserved) throw new Error("expected reservation");

    expect(await driver.remove("default", reserved.id, 1, 500)).toBe(true);
    expect(await driver.reserve("default", 500, 1_500)).toBeNull();
  });
});

describe("failed jobs", () => {
  it("continues restoring valid records after a malformed body is removed", async () => {
    const { client, driver } = setup();
    const failJob = async (payload: string) => {
      await driver.insert("default", payload, 0, 0);
      const reserved = await driver.reserve("default", 0, 1_000);
      if (!reserved) throw new Error(`expected reservation for ${payload}`);
      expect(await driver.fail("default", reserved.id, 1, 0, "failed")).toBe(true);
    };

    await failJob("A");
    await client.sendMessage({ QueueUrl: FAILED_URL, MessageBody: "poison" });
    await failJob("B");
    await failJob("C");

    await expect(driver.failedJobs(null)).rejects.toBeInstanceOf(AggregateError);
    const page = await client.receiveMessage({ QueueUrl: FAILED_URL, MaxNumberOfMessages: 10, VisibilityTimeout: 0 });
    const poison = page.Messages?.find((message) => message.Body === "poison");
    if (!poison || typeof poison.ReceiptHandle !== "string") throw new Error("expected poison message");
    await client.deleteMessage({ QueueUrl: FAILED_URL, ReceiptHandle: poison.ReceiptHandle });

    expect(await driver.failedJobs(null)).toHaveLength(3);
  });

  it("retries failed visibility restoration so the next scan sees every record", async () => {
    const { client, driver } = setup();
    for (let index = 0; index < 3; index += 1) {
      await driver.insert("default", `job-${index}`, 0, 0);
      const reserved = await driver.reserve("default", 0, 1_000);
      if (!reserved) throw new Error(`expected reservation ${index}`);
      expect(await driver.fail("default", reserved.id, 1, 0, "failed")).toBe(true);
    }

    client.failNextChangeVisibility = true;
    await expect(driver.failedJobs(null)).rejects.toMatchObject({
      name: "AggregateError",
      errors: expect.arrayContaining([expect.objectContaining({ message: "injected visibility restore failure" })]),
    });
    expect(await driver.failedJobs(null)).toHaveLength(3);
  });

  it("reports both a page-read error and a per-record visibility restore error", async () => {
    const { client, driver } = setup();
    for (let index = 0; index < 11; index += 1) {
      await driver.insert("default", `job-${index}`, 0, 0);
      const reserved = await driver.reserve("default", 0, 1_000);
      if (!reserved) throw new Error(`expected reservation ${index}`);
      expect(await driver.fail("default", reserved.id, 1, 0, "failed")).toBe(true);
    }

    client.failReceiveAfter = 1;
    client.failNextChangeVisibility = true;
    await expect(driver.failedJobs(null)).rejects.toMatchObject({
      name: "AggregateError",
      errors: expect.arrayContaining([
        expect.objectContaining({ message: "injected receive failure" }),
        expect.objectContaining({ message: "injected visibility restore failure" }),
      ]),
    });
    expect(await driver.failedJobs(null)).toHaveLength(11);
    client.clock = MAX_LEASE_MS;
    expect(await driver.failedJobs(null)).toHaveLength(11);
  });

  it("restores first-page visibility when a later failed-store receive fails", async () => {
    const { client, driver } = setup();
    for (let index = 0; index < 11; index += 1) {
      await driver.insert("default", `job-${index}`, 0, 0);
      const reserved = await driver.reserve("default", 0, 1_000);
      if (!reserved) throw new Error(`expected reservation ${index}`);
      expect(await driver.fail("default", reserved.id, 1, 0, "failed")).toBe(true);
    }

    client.failReceiveAfter = 1;
    await expect(driver.failedJobs(null)).rejects.toMatchObject({
      name: "AggregateError",
      errors: expect.arrayContaining([expect.objectContaining({ message: "injected receive failure" })]),
    });
    client.failReceiveAfter = null;
    expect(await driver.failedJobs(null)).toHaveLength(11);
  });

  it("retryAllFailed processes all failed jobs across SQS receive pages", async () => {
    const { driver } = setup();
    for (let index = 0; index < 11; index += 1) {
      await driver.insert("default", `job-${index}`, 0, 0);
      const reserved = await driver.reserve("default", 0, 1_000);
      if (!reserved) throw new Error(`expected reservation ${index}`);
      expect(await driver.fail("default", reserved.id, 1, 0, "failed")).toBe(true);
    }

    expect(await driver.retryAllFailed(null, 0)).toBe(11);
    expect(await driver.failedJobs(null)).toHaveLength(0);
  });
  it("failed jobs are visible to a separate driver sharing the SQS failed queue", async () => {
    const client = new FakeSqsClient();
    const options = { client, queueUrl: (queue: string) => `https://sqs.test/${queue}`, failedQueueUrl: FAILED_URL };
    const first = new SqsQueueDriver(options);
    const second = new SqsQueueDriver(options);
    await first.insert("default", "{}", 0, 0);
    const reserved = await first.reserve("default", 0, 1_000);
    if (!reserved) throw new Error("expected reservation");
    expect(await first.fail("default", reserved.id, 1, 100, "boom")).toBe(true);

    expect(await second.failedJobs("default")).toEqual([
      expect.objectContaining({ queue: "default", payload: "{}", error: "boom" }),
    ]);
  });
  it("records a failed job and retryFailed restores it with attempts reset", async () => {
    const { client, driver } = setup();
    const id = await driver.insert("default", '{"n":1}', 0, 0);
    const reserved = await driver.reserve("default", 0, 1_000);
    if (!reserved) throw new Error("expected reservation");

    client.clock = 100;
    expect(await driver.fail("default", reserved.id, 1, 100, "boom")).toBe(true);
    expect(await driver.failedJobs("default")).toEqual([
      { id, queue: "default", payload: '{"n":1}', attempts: 1, error: "boom", failedAt: 100 },
    ]);
    expect(await driver.reserve("default", 100, 1_100)).toBeNull();

    client.clock = 200;
    expect(await driver.retryFailed("unknown", 200)).toBe(false);
    expect(await driver.retryFailed(id, 200)).toBe(true);
    expect(await driver.failedJobs(null)).toEqual([]);
    expect(await driver.reserve("default", 200, 1_200)).toEqual({
      id: expect.any(String),
      queue: "default",
      payload: '{"n":1}',
      attempts: 1,
    });
  });

  it("retryAllFailed moves matching records and pruneFailed drops old ones", async () => {
    const { client, driver } = setup();
    await driver.insert("emails", "{}", 0, 0);
    await driver.insert("reports", "{}", 0, 0);
    const email = await driver.reserve("emails", 0, 1_000);
    const report = await driver.reserve("reports", 0, 1_000);
    if (!email || !report) throw new Error("expected reservations");
    client.clock = 500;
    await driver.fail("emails", email.id, 1, 100, "old");
    await driver.fail("reports", report.id, 1, 500, "new");

    expect(await driver.pruneFailed(200)).toBe(1);
    expect((await driver.failedJobs(null)).map((f) => f.queue)).toEqual(["reports"]);
    expect(await driver.retryAllFailed(null, 600)).toBe(1);
    expect(await driver.failedJobs(null)).toEqual([]);
    expect(await driver.reserve("reports", 600, 1_600)).toMatchObject({ attempts: 1 });
  });

  it("retryFailed and forgetFailed delete using a real failed-message receipt handle", async () => {
    const { driver } = setup();
    await driver.insert("default", "{}", 0, 0);
    const reserved = await driver.reserve("default", 0, 1_000);
    if (!reserved) throw new Error("expected reservation");
    await driver.fail("default", reserved.id, 1, 0, "failed");
    const [failed] = await driver.failedJobs("default");
    expect(await driver.retryFailed(failed.id, 10)).toBe(true);
    await driver.insert("default", "{}", 0, 0);
    const next = await driver.reserve("default", 10, 1_000);
    if (!next) throw new Error("expected next reservation");
    await driver.fail("default", next.id, 1, 10, "failed again");
    const [secondFailure] = await driver.failedJobs("default");
    expect(await driver.forgetFailed(secondFailure.id)).toBe(true);
  });

  it("flushFailed and forgetFailed delete failed records", async () => {
    const { client, driver } = setup();
    await driver.insert("emails", "{}", 0, 0);
    await driver.insert("emails", "{}", 0, 0);
    const a = await driver.reserve("emails", 0, 1_000);
    const b = await driver.reserve("emails", 0, 1_000);
    if (!a || !b) throw new Error("expected reservations");
    client.clock = 0;
    await driver.fail("emails", a.id, 1, 0, "x");
    await driver.fail("emails", b.id, 1, 0, "y");

    const failed = await driver.failedJobs("emails");
    expect(await driver.forgetFailed(failed[0].id)).toBe(true);
    expect(await driver.forgetFailed(failed[0].id)).toBe(false);
    expect(await driver.flushFailed("emails")).toBe(1);
    expect(await driver.failedJobs(null)).toEqual([]);
  });
});

describe("counts", () => {
  it("counts pending, delayed and reserved jobs as of now", async () => {
    const { client, driver } = setup();
    client.clock = 0;
    await driver.insert("default", "a", 0, 0);
    await driver.insert("default", "b", 5_000, 0);
    const held = await driver.reserve("default", 0, 1_000);
    if (!held) throw new Error("expected reservation");
    await driver.insert("default", "c", 0, 0);

    expect(await driver.counts("default", 0)).toEqual({ pending: 1, delayed: 1, reserved: 1 });
  });
});

describe("visibility timeout", () => {
  it("rejects a lease above 12h before receiving", async () => {
    const { client, driver } = setup();
    await driver.insert("default", "{}", 0, 0);

    await expect(driver.reserve("default", 0, MAX_LEASE_MS + 1_000)).rejects.toThrow(/12h/);
    expect(client.calls.filter((c) => c.op === "ReceiveMessage")).toHaveLength(0);
  });
});
