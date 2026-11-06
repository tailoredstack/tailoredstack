import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { SqliteQueueDriver } from "../src/drivers/sqlite.ts";

let dir: string;
let path: string;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "queue-sqlite-"));
  path = join(dir, "queue.db");
});

afterEach(() => {
  rmSync(dir, { recursive: true, force: true });
});

describe("insert and reserve", () => {
  it("returns an inserted job on reserve with attempts 1", async () => {
    const driver = new SqliteQueueDriver({ path });
    const id = await driver.insert("default", '{"n":1}', 0);

    expect(await driver.reserve("default", 100, 1_000)).toEqual({
      id,
      queue: "default",
      payload: '{"n":1}',
      attempts: 1,
    });
  });

  it("returns null when the queue is empty or the job is not yet available", async () => {
    const driver = new SqliteQueueDriver({ path });
    await driver.insert("default", "{}", 500);

    expect(await driver.reserve("default", 100, 1_000)).toBeNull();
    expect(await driver.reserve("other", 600, 1_000)).toBeNull();
  });

  it("reserves oldest available first, then insertion order", async () => {
    const driver = new SqliteQueueDriver({ path });
    const a = await driver.insert("default", "a", 100);
    const b = await driver.insert("default", "b", 50);
    const c = await driver.insert("default", "c", 50);

    const order: string[] = [];
    for (let i = 0; i < 3; i++) {
      const job = await driver.reserve("default", 200, 1_000);
      if (job) order.push(job.id);
    }

    expect(order).toEqual([b, c, a]);
  });

  it("hides a reserved job until reservedUntil, then makes it ready again", async () => {
    const driver = new SqliteQueueDriver({ path });
    const id = await driver.insert("default", "{}", 0);
    await driver.reserve("default", 1_000, 1_500);

    expect(await driver.reserve("default", 1_200, 2_000)).toBeNull();

    expect(await driver.reserve("default", 1_500, 2_000)).toEqual({
      id,
      queue: "default",
      payload: "{}",
      attempts: 2,
    });
  });
});

describe("lease checks on remove, release, and fail", () => {
  it("remove succeeds with the current attempts and an unexpired lease", async () => {
    const driver = new SqliteQueueDriver({ path });
    const id = await driver.insert("default", "{}", 0);
    await driver.reserve("default", 1_000, 1_500);

    expect(await driver.remove("default", id, 1, 1_200)).toBe(true);
    expect(await driver.counts("default", 1_200)).toEqual({ pending: 0, delayed: 0, reserved: 0 });
  });

  it("remove, release, and fail return false for a stale attempt count", async () => {
    const driver = new SqliteQueueDriver({ path });
    const id = await driver.insert("default", "{}", 0);
    await driver.reserve("default", 1_000, 1_500);
    await driver.reserve("default", 1_500, 2_500); // attempts is now 2

    expect(await driver.remove("default", id, 1, 1_600)).toBe(false);
    expect(await driver.release("default", id, 1, 1_600, 1_600)).toBe(false);
    expect(await driver.fail("default", id, 1, 1_600, "boom")).toBe(false);
    expect(await driver.failedJobs(null)).toEqual([]);
  });

  it("remove, release, and fail return false once the lease has expired", async () => {
    const driver = new SqliteQueueDriver({ path });
    const id = await driver.insert("default", "{}", 0);
    await driver.reserve("default", 1_000, 1_500);

    expect(await driver.remove("default", id, 1, 1_500)).toBe(false);
    expect(await driver.release("default", id, 1, 1_500, 1_500)).toBe(false);
    expect(await driver.fail("default", id, 1, 1_500, "boom")).toBe(false);
    expect(await driver.counts("default", 1_500)).toEqual({ pending: 1, delayed: 0, reserved: 0 });
  });

  it("remove returns false for a job on another queue", async () => {
    const driver = new SqliteQueueDriver({ path });
    const id = await driver.insert("default", "{}", 0);
    await driver.reserve("default", 1_000, 1_500);

    expect(await driver.remove("other", id, 1, 1_200)).toBe(false);
  });

  it("release makes the job ready at availableAt under the same reservation", async () => {
    const driver = new SqliteQueueDriver({ path });
    const id = await driver.insert("default", "{}", 0);
    await driver.reserve("default", 1_000, 1_500);

    expect(await driver.release("default", id, 1, 1_100, 1_300)).toBe(true);
    expect(await driver.reserve("default", 1_200, 2_000)).toBeNull();
    expect(await driver.reserve("default", 1_300, 2_000)).toMatchObject({ id, attempts: 2 });
  });
});

describe("fail and failed jobs", () => {
  it("moves the held job into failed_jobs with error, attempts, and failedAt", async () => {
    const driver = new SqliteQueueDriver({ path });
    const id = await driver.insert("emails", '{"to":"a"}', 0);
    await driver.reserve("emails", 1_000, 1_500);

    expect(await driver.fail("emails", id, 1, 1_200, "smtp down")).toBe(true);

    expect(await driver.failedJobs(null)).toEqual([
      { id, queue: "emails", payload: '{"to":"a"}', attempts: 1, error: "smtp down", failedAt: 1_200 },
    ]);
    expect(await driver.counts("emails", 1_200)).toEqual({ pending: 0, delayed: 0, reserved: 0 });
  });

  it("filters failedJobs by queue", async () => {
    const driver = new SqliteQueueDriver({ path });
    const a = await driver.insert("emails", "{}", 0);
    const b = await driver.insert("reports", "{}", 0);
    await driver.reserve("emails", 1_000, 1_500);
    await driver.reserve("reports", 1_000, 1_500);
    await driver.fail("emails", a, 1, 1_100, "x");
    await driver.fail("reports", b, 1, 1_100, "y");

    expect((await driver.failedJobs("emails")).map((f) => f.id)).toEqual([a]);
    expect((await driver.failedJobs(null)).map((f) => f.id).sort()).toEqual([a, b].sort());
  });

  it("retryFailed puts one job back ready now with attempts reset", async () => {
    const driver = new SqliteQueueDriver({ path });
    const id = await driver.insert("default", "{}", 0);
    await driver.reserve("default", 1_000, 1_500);
    await driver.fail("default", id, 1, 1_100, "x");

    expect(await driver.retryFailed(id, 2_000)).toBe(true);
    expect(await driver.failedJobs(null)).toEqual([]);
    expect(await driver.reserve("default", 2_000, 3_000)).toMatchObject({ id, attempts: 1 });
  });

  it("retryFailed returns false for an unknown id", async () => {
    const driver = new SqliteQueueDriver({ path });

    expect(await driver.retryFailed("999", 2_000)).toBe(false);
  });

  it("retryAllFailed restores every job on one queue, or on all queues, and returns the count", async () => {
    const driver = new SqliteQueueDriver({ path });
    const a = await driver.insert("emails", "{}", 0);
    const b = await driver.insert("emails", "{}", 0);
    const c = await driver.insert("reports", "{}", 0);
    await driver.reserve("emails", 1_000, 1_500);
    await driver.reserve("emails", 1_000, 1_500);
    await driver.reserve("reports", 1_000, 1_500);
    await driver.fail("emails", a, 1, 1_100, "x");
    await driver.fail("emails", b, 1, 1_100, "x");
    await driver.fail("reports", c, 1, 1_100, "x");

    expect(await driver.retryAllFailed("emails", 2_000)).toBe(2);
    expect((await driver.failedJobs(null)).map((f) => f.id)).toEqual([c]);
    expect(await driver.counts("emails", 2_000)).toEqual({ pending: 2, delayed: 0, reserved: 0 });

    expect(await driver.retryAllFailed(null, 2_000)).toBe(1);
    expect(await driver.failedJobs(null)).toEqual([]);
    expect(await driver.reserve("reports", 2_000, 3_000)).toMatchObject({ id: c, attempts: 1 });
  });

  it("forgetFailed deletes one record and returns false when it is gone", async () => {
    const driver = new SqliteQueueDriver({ path });
    const id = await driver.insert("default", "{}", 0);
    await driver.reserve("default", 1_000, 1_500);
    await driver.fail("default", id, 1, 1_100, "x");

    expect(await driver.forgetFailed(id)).toBe(true);
    expect(await driver.forgetFailed(id)).toBe(false);
    expect(await driver.failedJobs(null)).toEqual([]);
  });

  it("flushFailed deletes failed records on one queue, or on all queues, and returns the count", async () => {
    const driver = new SqliteQueueDriver({ path });
    const a = await driver.insert("emails", "{}", 0);
    const b = await driver.insert("reports", "{}", 0);
    const c = await driver.insert("reports", "{}", 0);
    await driver.reserve("emails", 1_000, 1_500);
    await driver.reserve("reports", 1_000, 1_500);
    await driver.reserve("reports", 1_000, 1_500);
    await driver.fail("emails", a, 1, 1_100, "x");
    await driver.fail("reports", b, 1, 1_100, "x");
    await driver.fail("reports", c, 1, 1_100, "x");

    expect(await driver.flushFailed("reports")).toBe(2);
    expect((await driver.failedJobs(null)).map((f) => f.id)).toEqual([a]);
    expect(await driver.flushFailed(null)).toBe(1);
    expect(await driver.failedJobs(null)).toEqual([]);
  });

  it("pruneFailed deletes failed records recorded before the cutoff and returns the count", async () => {
    const driver = new SqliteQueueDriver({ path });
    const early = await driver.insert("default", "{}", 0);
    const late = await driver.insert("default", "{}", 0);
    await driver.reserve("default", 1_000, 1_500);
    await driver.fail("default", early, 1, 1_000, "x");
    await driver.reserve("default", 3_000, 3_500);
    await driver.fail("default", late, 1, 3_000, "x");

    expect(await driver.pruneFailed(2_000)).toBe(1);
    expect((await driver.failedJobs(null)).map((f) => [f.id, f.failedAt])).toEqual([[late, 3_000]]);
  });
});

describe("counts", () => {
  it("classifies jobs as reserved, delayed, or pending on one queue", async () => {
    const driver = new SqliteQueueDriver({ path });
    await driver.insert("default", "ready", 0);
    await driver.insert("default", "later", 5_000);
    await driver.insert("default", "held", 0);
    await driver.insert("other", "elsewhere", 0);
    await driver.reserve("default", 1_000, 2_000); // takes "ready" (oldest, id order)

    expect(await driver.counts("default", 1_000)).toEqual({ pending: 1, delayed: 1, reserved: 1 });
    expect(await driver.counts("default", 2_000)).toEqual({ pending: 2, delayed: 1, reserved: 0 });
  });
});

describe("concurrency", () => {
  it("two driver instances on one file never reserve the same job", async () => {
    const seed = new SqliteQueueDriver({ path });
    const ids = new Set<string>();
    for (let i = 0; i < 60; i++) ids.add(await seed.insert("default", String(i), 0));

    const a = new SqliteQueueDriver({ path });
    const b = new SqliteQueueDriver({ path });
    const drivers = [a, b];
    const reserved = await Promise.all(
      Array.from({ length: 80 }, (_, i) => drivers[i % 2]!.reserve("default", 1_000, 2_000)),
    );

    const got = reserved.filter((job) => job !== null).map((job) => job.id);
    expect(got).toHaveLength(60);
    expect(new Set(got).size).toBe(60);
    expect(new Set(got)).toEqual(ids);
  });
});
