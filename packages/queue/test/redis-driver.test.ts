import { randomUUID } from "node:crypto";
import { RedisContainer, type StartedRedisContainer } from "@testcontainers/redis";
import { Redis } from "ioredis";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { RedisQueueDriver } from "../src/drivers/redis.ts";

// One real Redis for this file, started with testcontainers (same setup as the cache's test/global-setup.ts).
let container: StartedRedisContainer;
let url: string;
const clients: Redis[] = [];

beforeAll(async () => {
  container = await new RedisContainer("redis:8-alpine").start();
  url = container.getConnectionUrl();
}, 120_000);

afterAll(async () => {
  for (const client of clients) client.disconnect();
  await container?.stop();
});

/** A driver on its own key namespace, so tests never see each other's jobs. */
function freshDriver(): { driver: RedisQueueDriver; client: Redis } {
  const client = new Redis(url, { maxRetriesPerRequest: 1 });
  clients.push(client);
  return { driver: new RedisQueueDriver({ client, prefix: `queue:${randomUUID()}:` }), client };
}

describe("insert and reserve", () => {
  it("returns an inserted job on reserve with attempts 1", async () => {
    const { driver } = freshDriver();
    const id = await driver.insert("default", '{"n":1}', 0);

    expect(await driver.reserve("default", 100, 1_000)).toEqual({
      id,
      queue: "default",
      payload: '{"n":1}',
      attempts: 1,
    });
  });

  it("returns null when the queue is empty or the job is not yet available", async () => {
    const { driver } = freshDriver();
    await driver.insert("default", "{}", 500);

    expect(await driver.reserve("default", 100, 1_000)).toBeNull();
    expect(await driver.reserve("other", 600, 1_000)).toBeNull();
  });
});

describe("FIFO", () => {
  it("reserves the job with the earliest availability first", async () => {
    const { driver } = freshDriver();
    const late = await driver.insert("q", "late", 200);
    const early = await driver.insert("q", "early", 100);

    expect((await driver.reserve("q", 300, 1_000))?.id).toBe(early);
    expect((await driver.reserve("q", 300, 1_000))?.id).toBe(late);
  });

  it("keeps insertion order for equal availability, past nine jobs", async () => {
    const { driver } = freshDriver();
    const ids: string[] = [];
    for (let i = 0; i < 12; i++) ids.push(await driver.insert("q", `${i}`, 100));

    const reserved: string[] = [];
    for (let i = 0; i < 12; i++) reserved.push((await driver.reserve("q", 200, 1_000))?.id ?? "missing");
    expect(reserved).toEqual(ids);
  });
});

describe("lease", () => {
  it("hides a reserved job until its lease ends, then makes it ready with attempts counted", async () => {
    const { driver } = freshDriver();
    const id = await driver.insert("q", "{}", 0);
    await driver.reserve("q", 100, 1_000);

    expect(await driver.reserve("q", 999, 2_000)).toBeNull();
    expect(await driver.reserve("q", 1_000, 2_000)).toEqual({ id, queue: "q", payload: "{}", attempts: 2 });
  });
});

describe("expired lease keeps its place", () => {
  it("returns an expired job before newer ready jobs, in its original availability order", async () => {
    const { driver } = freshDriver();
    const early = await driver.insert("q", "early", 100);
    const late = await driver.insert("q", "late", 200);

    expect((await driver.reserve("q", 300, 1_000))?.id).toBe(early); // leases early until 1000
    expect((await driver.reserve("q", 300, 1_000))?.id).toBe(late); // leases late until 1000
    expect(await driver.reserve("q", 1_000, 2_000)).toMatchObject({ id: early, attempts: 2 });
  });
});

describe("stale attempts", () => {
  it("remove rejects a stale attempt and a lease that has ended", async () => {
    const { driver } = freshDriver();
    const id = await driver.insert("q", "{}", 0);
    await driver.reserve("q", 100, 1_000);
    await driver.reserve("q", 1_000, 2_000); // attempt 2 after the first lease ended

    expect(await driver.remove("q", id, 1, 1_000)).toBe(false);
    expect(await driver.remove("q", id, 2, 2_000)).toBe(false);
    expect(await driver.remove("q", id, 2, 1_500)).toBe(true);
  });

  it("release rejects a stale attempt and makes a held job ready at availableAt", async () => {
    const { driver } = freshDriver();
    const id = await driver.insert("q", "{}", 0);
    await driver.reserve("q", 100, 1_000);

    expect(await driver.release("q", id, 0, 100, 500)).toBe(false);
    expect(await driver.release("q", id, 1, 100, 500)).toBe(true);
    expect(await driver.reserve("q", 499, 2_000)).toBeNull();
    expect((await driver.reserve("q", 500, 2_000))?.attempts).toBe(2);
  });

  it("fail rejects a stale attempt and records a held job", async () => {
    const { driver } = freshDriver();
    const id = await driver.insert("q", '{"x":1}', 0);
    await driver.reserve("q", 100, 1_000);

    expect(await driver.fail("q", id, 5, 200, "boom")).toBe(false);
    expect(await driver.fail("q", id, 1, 200, "boom")).toBe(true);
    expect(await driver.failedJobs("q")).toEqual([
      { id, queue: "q", payload: '{"x":1}', attempts: 1, error: "boom", failedAt: 200 },
    ]);
  });
});

describe("failed jobs", () => {
  it("retryFailed restores a failed job ready now with attempts reset", async () => {
    const { driver } = freshDriver();
    const id = await driver.insert("q", "{}", 0);
    await driver.reserve("q", 100, 1_000);
    await driver.fail("q", id, 1, 200, "boom");

    expect(await driver.retryFailed(id, 5_000)).toBe(true);
    expect(await driver.failedJobs(null)).toEqual([]);
    expect(await driver.reserve("q", 5_000, 6_000)).toEqual({ id, queue: "q", payload: "{}", attempts: 1 });
    expect(await driver.retryFailed("999999", 5_000)).toBe(false);
  });

  it("retryAllFailed, forgetFailed, and flushFailed act on the failed store", async () => {
    const { driver } = freshDriver();
    const a = await driver.insert("a", "{}", 0);
    const b = await driver.insert("b", "{}", 0);
    const c = await driver.insert("b", "{}", 0);
    for (const [queue, id] of [
      ["a", a],
      ["b", b],
      ["b", c],
    ] as const) {
      await driver.reserve(queue, 100, 1_000);
      await driver.fail(queue, id, 1, 200, "boom");
    }

    expect(await driver.retryAllFailed("b", 300)).toBe(2);
    expect((await driver.failedJobs(null)).map((f) => f.id)).toEqual([a]);
    expect(await driver.forgetFailed(a)).toBe(true);
    expect(await driver.forgetFailed(a)).toBe(false);
    expect(await driver.flushFailed(null)).toBe(0);
  });
});

describe("counts", () => {
  it("counts pending, delayed, and reserved jobs on one queue as of now", async () => {
    const { driver } = freshDriver();
    await driver.insert("q", "ready", 0);
    await driver.insert("q", "delayed", 500);
    await driver.insert("other", "elsewhere", 0);
    await driver.insert("q", "reserved", 0);
    await driver.reserve("q", 100, 1_000); // takes "ready" (oldest) and holds it

    expect(await driver.counts("q", 100)).toEqual({ pending: 1, delayed: 1, reserved: 1 });
    expect(await driver.counts("q", 1_000)).toEqual({ pending: 3, delayed: 0, reserved: 0 });
  });
});

describe("wrong queue", () => {
  it("remove, release, and fail reject a held job when the queue name differs", async () => {
    const { driver } = freshDriver();
    const id = await driver.insert("q", "{}", 0);
    await driver.reserve("q", 100, 1_000);

    expect(await driver.remove("other", id, 1, 200)).toBe(false);
    expect(await driver.release("other", id, 1, 200, 300)).toBe(false);
    expect(await driver.fail("other", id, 1, 200, "boom")).toBe(false);
    expect(await driver.failedJobs(null)).toEqual([]);
    expect(await driver.counts("q", 200)).toEqual({ pending: 0, delayed: 0, reserved: 1 });
    expect(await driver.remove("q", id, 1, 200)).toBe(true);
  });
});

describe("ended lease", () => {
  it("release and fail reject a lease that ended at the same attempt", async () => {
    const { driver } = freshDriver();
    const id = await driver.insert("q", "{}", 0);
    await driver.reserve("q", 100, 1_000);

    expect(await driver.release("q", id, 1, 1_000, 1_500)).toBe(false);
    expect(await driver.fail("q", id, 1, 1_000, "late")).toBe(false);
    expect(await driver.failedJobs(null)).toEqual([]);
  });
});

describe("failed store ordering, pruning, and unknown ids", () => {
  it("lists failed jobs by failedAt, prunes strictly before the cutoff, and forgets unknown ids as false", async () => {
    const { driver } = freshDriver();
    const a = await driver.insert("q", "a", 0);
    const b = await driver.insert("q", "b", 0);
    await driver.reserve("q", 100, 1_000); // a
    await driver.reserve("q", 100, 1_000); // b
    expect(await driver.fail("q", b, 1, 200, "b-boom")).toBe(true);
    expect(await driver.fail("q", a, 1, 300, "a-boom")).toBe(true);

    expect((await driver.failedJobs(null)).map((f) => [f.id, f.failedAt])).toEqual([
      [b, 200],
      [a, 300],
    ]);
    expect(await driver.pruneFailed(250)).toBe(1); // b at 200 goes, a at 300 stays
    expect((await driver.failedJobs(null)).map((f) => f.id)).toEqual([a]);
    expect(await driver.pruneFailed(250)).toBe(0); // a at 300 is not before the cutoff
    expect(await driver.forgetFailed("999999")).toBe(false);
  });
});

describe("all-queue variants", () => {
  it("retryAllFailed(null) and flushFailed(null) act across every queue", async () => {
    const { driver } = freshDriver();
    const a = await driver.insert("a", "{}", 0);
    const b = await driver.insert("b", "{}", 0);
    await driver.reserve("a", 100, 1_000);
    await driver.reserve("b", 100, 1_000);
    await driver.fail("a", a, 1, 200, "boom");
    await driver.fail("b", b, 1, 200, "boom");

    expect(await driver.retryAllFailed(null, 500)).toBe(2);
    expect(await driver.failedJobs(null)).toEqual([]);
    expect(await driver.reserve("a", 500, 1_000)).toMatchObject({ id: a, attempts: 1 });
    expect(await driver.reserve("b", 500, 1_000)).toMatchObject({ id: b, attempts: 1 });

    await driver.fail("a", a, 1, 600, "again");
    await driver.fail("b", b, 1, 600, "again");
    expect(await driver.flushFailed(null)).toBe(2);
    expect(await driver.failedJobs(null)).toEqual([]);
  });
});

describe("odd names and payloads", () => {
  it("keeps queues whose names contain colons apart", async () => {
    const { driver } = freshDriver();
    const colon = await driver.insert("a:b", "x", 0);

    expect(await driver.reserve("a", 100, 1_000)).toBeNull();
    expect(await driver.reserve("a:b", 100, 1_000)).toMatchObject({ id: colon, queue: "a:b" });
  });

  it("round-trips empty, delimiter, newline, and unicode payloads in insertion order", async () => {
    const { driver } = freshDriver();
    const payloads = ["", "a|b:c", "line1\nline2", "héllo 🙂", '{"x":"\\u0000"}'];
    for (const payload of payloads) await driver.insert("q", payload, 0);

    const got: string[] = [];
    for (let i = 0; i < payloads.length; i++) got.push((await driver.reserve("q", 100, 1_000))?.payload ?? "MISSING");
    expect(got).toEqual(payloads);
  });

  it("counts zero on a queue that was never used", async () => {
    const { driver } = freshDriver();
    expect(await driver.counts("never-used", 100)).toEqual({ pending: 0, delayed: 0, reserved: 0 });
  });
});

describe("race on one held job", () => {
  it("when release and remove race for the same held job, exactly one wins and no copy is left behind", async () => {
    const shared = `queue:${randomUUID()}:`;
    const a = new Redis(url, { maxRetriesPerRequest: 1 });
    const b = new Redis(url, { maxRetriesPerRequest: 1 });
    clients.push(a, b);
    const driverA = new RedisQueueDriver({ client: a, prefix: shared });
    const driverB = new RedisQueueDriver({ client: b, prefix: shared });

    const id = await driverA.insert("q", "{}", 0);
    await driverA.reserve("q", 100, 10_000);
    const [released, removed] = await Promise.all([
      driverA.release("q", id, 1, 200, 300),
      driverB.remove("q", id, 1, 200),
    ]);

    expect(Number(released) + Number(removed)).toBe(1);
    expect(await driverA.failedJobs(null)).toEqual([]);
    const total = (await driverA.counts("q", 400)).pending;
    expect(total).toBe(released ? 1 : 0);
  });

  it("when remove and fail race for the same held job, exactly one wins", async () => {
    const shared = `queue:${randomUUID()}:`;
    const a = new Redis(url, { maxRetriesPerRequest: 1 });
    const b = new Redis(url, { maxRetriesPerRequest: 1 });
    clients.push(a, b);
    const driverA = new RedisQueueDriver({ client: a, prefix: shared });
    const driverB = new RedisQueueDriver({ client: b, prefix: shared });

    const id = await driverA.insert("q", "{}", 0);
    await driverA.reserve("q", 100, 10_000);
    const [removed, failed] = await Promise.all([
      driverA.remove("q", id, 1, 200),
      driverB.fail("q", id, 1, 200, "boom"),
    ]);

    expect(Number(removed) + Number(failed)).toBe(1);
    expect((await driverA.failedJobs(null)).length).toBe(failed ? 1 : 0);
    expect(await driverA.counts("q", 200)).toEqual({ pending: 0, delayed: 0, reserved: 0 });
  });
});

describe("two clients on one Redis", () => {
  it("never reserves the same job twice", async () => {
    const shared = `queue:${randomUUID()}:`;
    const a = new Redis(url, { maxRetriesPerRequest: 1 });
    const b = new Redis(url, { maxRetriesPerRequest: 1 });
    clients.push(a, b);
    const driverA = new RedisQueueDriver({ client: a, prefix: shared });
    const driverB = new RedisQueueDriver({ client: b, prefix: shared });

    const total = 200;
    for (let i = 0; i < total; i++) await driverA.insert("q", `${i}`, 0);

    const drain = async (driver: RedisQueueDriver) => {
      const ids: string[] = [];
      for (;;) {
        const job = await driver.reserve("q", 100, 10_000);
        if (job === null) return ids;
        ids.push(job.id);
      }
    };
    const [fromA, fromB] = await Promise.all([drain(driverA), drain(driverB)]);

    const all = [...fromA, ...fromB];
    expect(all).toHaveLength(total);
    expect(new Set(all).size).toBe(total);
  });
});
