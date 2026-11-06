import { describe, expect, it } from "vitest";
import { SqsQueueDriver } from "../src/drivers/sqs.ts";
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

describe("insert and reserve", () => {
  it("returns an inserted job on reserve with attempts 1", async () => {
    const { driver } = setup();
    const id = await driver.insert("default", '{"n":1}', 0, 0);

    expect(await driver.reserve("default", 0, 1_000)).toEqual({
      id,
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
    const id = await driver.insert("default", "{}", 0, 0);

    expect(await driver.reserve("default", 0, 1_000)).toMatchObject({ id, attempts: 1 });

    client.clock = 999;
    expect(await driver.reserve("default", 999, 2_000)).toBeNull();

    client.clock = 1_000;
    expect(await driver.reserve("default", 1_000, 2_000)).toEqual({ id, queue: "default", payload: "{}", attempts: 2 });
  });
});

describe("stale attempts", () => {
  it("remove, release and fail return false for an attempt that no longer holds the job", async () => {
    const { client, driver } = setup();
    const id = await driver.insert("default", "{}", 0, 0);
    await driver.reserve("default", 0, 1_000);

    client.clock = 1_000;
    await driver.reserve("default", 1_000, 2_000); // attempt 2 now holds the job

    expect(await driver.remove("default", id, 1, 1_500)).toBe(false);
    expect(await driver.release("default", id, 1, 1_500, 0)).toBe(false);
    expect(await driver.fail("default", id, 1, 1_500, "stale")).toBe(false);

    expect(client.calls.filter((c) => c.op === "DeleteMessage")).toHaveLength(0);
    expect(client.calls.filter((c) => c.op === "SendMessage" && c.input.QueueUrl === FAILED_URL)).toHaveLength(0);
  });

  it("remove returns false once the lease has expired, even for the current attempt", async () => {
    const { driver } = setup();
    const id = await driver.insert("default", "{}", 0, 0);
    await driver.reserve("default", 0, 1_000);

    expect(await driver.remove("default", id, 1, 1_000)).toBe(false);
  });

  it("the current holder can remove the job", async () => {
    const { driver } = setup();
    const id = await driver.insert("default", "{}", 0, 0);
    await driver.reserve("default", 0, 1_000);

    expect(await driver.remove("default", id, 1, 500)).toBe(true);
    expect(await driver.reserve("default", 500, 1_500)).toBeNull();
  });
});

describe("failed jobs", () => {
  it("records a failed job and retryFailed restores it with attempts reset", async () => {
    const { client, driver } = setup();
    const id = await driver.insert("default", '{"n":1}', 0, 0);
    await driver.reserve("default", 0, 1_000);

    client.clock = 100;
    expect(await driver.fail("default", id, 1, 100, "boom")).toBe(true);
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
    const e = await driver.insert("emails", "{}", 0, 0);
    const r = await driver.insert("reports", "{}", 0, 0);
    await driver.reserve("emails", 0, 1_000);
    await driver.reserve("reports", 0, 1_000);
    client.clock = 500;
    await driver.fail("emails", e, 1, 100, "old");
    await driver.fail("reports", r, 1, 500, "new");

    expect(await driver.pruneFailed(200)).toBe(1);
    expect((await driver.failedJobs(null)).map((f) => f.id)).toEqual([r]);
    expect(await driver.retryAllFailed(null, 600)).toBe(1);
    expect(await driver.failedJobs(null)).toEqual([]);
    expect(await driver.reserve("reports", 600, 1_600)).toMatchObject({ attempts: 1 });
  });

  it("flushFailed and forgetFailed delete failed records", async () => {
    const { client, driver } = setup();
    const a = await driver.insert("emails", "{}", 0, 0);
    const b = await driver.insert("emails", "{}", 0, 0);
    await driver.reserve("emails", 0, 1_000);
    await driver.reserve("emails", 0, 1_000);
    client.clock = 0;
    await driver.fail("emails", a, 1, 0, "x");
    await driver.fail("emails", b, 1, 0, "y");

    expect(await driver.forgetFailed(a)).toBe(true);
    expect(await driver.forgetFailed(a)).toBe(false);
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
    await driver.reserve("default", 0, 1_000);
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
