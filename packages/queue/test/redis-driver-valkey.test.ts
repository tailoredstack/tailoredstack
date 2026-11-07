import { randomUUID } from "node:crypto";
import { type StartedValkeyContainer, ValkeyContainer } from "@testcontainers/valkey";
import { Redis } from "ioredis";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { RedisQueueDriver } from "../src/drivers/redis.ts";
import { Queue } from "../src/queue.ts";

// The same driver against a Valkey server, which speaks the Redis protocol. Each case here is a behaviour the
// Lua scripts depend on; if Valkey handled a script differently, one of these would fail.
let container: StartedValkeyContainer;
let url: string;
const clients: Redis[] = [];

beforeAll(async () => {
  container = await new ValkeyContainer("valkey/valkey:8-alpine").start();
  url = container.getConnectionUrl();
}, 120_000);

afterAll(async () => {
  for (const client of clients) client.disconnect();
  await container?.stop();
});

function freshDriver(): RedisQueueDriver {
  const client = new Redis(url, { maxRetriesPerRequest: 1 });
  clients.push(client);
  return new RedisQueueDriver({ client, prefix: `queue:${randomUUID()}:` });
}

describe("RedisQueueDriver on Valkey", () => {
  it("inserts, reserves in FIFO order, and hides a leased job until its lease ends", async () => {
    const driver = freshDriver();
    const late = await driver.insert("q", "late", 200);
    const early = await driver.insert("q", "early", 100);

    expect((await driver.reserve("q", 300, 1_000))?.id).toBe(early);
    expect((await driver.reserve("q", 300, 1_000))?.id).toBe(late);
    expect(await driver.reserve("q", 999, 2_000)).toBeNull();
    expect(await driver.reserve("q", 1_000, 2_000)).toMatchObject({ id: early, attempts: 2 });
  });

  it("rejects stale attempts on remove, release, and fail", async () => {
    const driver = freshDriver();
    const id = await driver.insert("q", "{}", 0);
    await driver.reserve("q", 100, 1_000);

    expect(await driver.remove("q", id, 9, 200)).toBe(false);
    expect(await driver.release("q", id, 9, 200, 300)).toBe(false);
    expect(await driver.fail("q", id, 9, 200, "boom")).toBe(false);
    expect(await driver.remove("q", id, 1, 200)).toBe(true);
  });

  it("records failed jobs, requeues them behind older ready jobs, and counts by state", async () => {
    const driver = freshDriver();
    const failed = await driver.insert("q", "f", 0);
    await driver.reserve("q", 100, 1_000);
    await driver.fail("q", failed, 1, 200, "boom");
    const older = await driver.insert("q", "older", 50);

    expect(await driver.counts("q", 300)).toEqual({ pending: 1, delayed: 0, reserved: 0 });
    expect(await driver.retryFailed(failed, 300)).toBe(true);
    expect((await driver.reserve("q", 300, 1_000))?.id).toBe(older);
    expect((await driver.reserve("q", 300, 1_000))?.id).toBe(failed);
  });

  it("two clients on one Valkey never reserve the same job", async () => {
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

  it("an expired lease is reserved again by a second client, with attempts 2", async () => {
    const shared = `queue:${randomUUID()}:`;
    const a = new Redis(url, { maxRetriesPerRequest: 1 });
    const b = new Redis(url, { maxRetriesPerRequest: 1 });
    clients.push(a, b);
    const driverA = new RedisQueueDriver({ client: a, prefix: shared });
    const driverB = new RedisQueueDriver({ client: b, prefix: shared });

    const id = await driverA.insert("q", "{}", 0);
    expect(await driverA.reserve("q", 100, 1_000)).toMatchObject({ id, attempts: 1 });
    expect(await driverB.reserve("q", 999, 2_000)).toBeNull();
    expect(await driverB.reserve("q", 1_000, 2_000)).toMatchObject({ id, attempts: 2 });
  });

  it("the core's lease reaches Valkey: Queue over RedisQueueDriver honours retryAfter", async () => {
    let now = 1_000;
    const queue = new Queue({ driver: freshDriver(), now: () => now, retryAfter: 5_000 });
    await queue.push({ name: "Flaky" });
    await queue.pop();
    now += 4_999;
    expect(await queue.pop()).toBeNull();
    now += 1;
    expect(await queue.pop()).toMatchObject({ name: "Flaky" });
  });
});
