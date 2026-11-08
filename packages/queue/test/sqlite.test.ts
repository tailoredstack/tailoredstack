import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it } from "vitest";
import { createQueue } from "../src/queue.js";
import { createSqliteDriver } from "../src/sqlite.js";
import { driverContract } from "../src/testing/driver-contract.js";

const dirs: string[] = [];
const admins: DatabaseSync[] = [];

afterEach(() => {
  for (const admin of admins.splice(0)) {
    admin.close();
  }
  for (const dir of dirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true });
  }
});

function tempPath(): string {
  const dir = mkdtempSync(join(tmpdir(), "queue-sqlite-"));
  dirs.push(dir);
  return join(dir, "queue.db");
}

// Two driver instances on one file: the shared-backend contract (B7) needs a peer.
driverContract({
  name: "sqlite",
  capabilities: { shared: true, exactCounts: true, maxPushDelayMs: Number.POSITIVE_INFINITY },
  fixture: async () => {
    const path = tempPath();
    return { driver: createSqliteDriver({ path }), peer: createSqliteDriver({ path }) };
  },
});

describe("sqlite driver backend faults", () => {
  it("rolls back a failed reserve and keeps the message ready (backend failure propagates unchanged)", async () => {
    const path = tempPath();
    const driver = createSqliteDriver({ path });
    const queue = createQueue(driver);
    const admin = new DatabaseSync(path);
    admins.push(admin);
    try {
      await queue.push("jobs", "body", { availableAt: 1_000, now: 1_000 });
      admin.exec(
        "CREATE TRIGGER fail_reserve BEFORE UPDATE OF attempts ON messages BEGIN SELECT RAISE(ABORT, 'disk full'); END;",
      );
      await expect(queue.reserve("jobs", 1_000, 2_000)).rejects.toThrow("disk full");
      admin.exec("DROP TRIGGER fail_reserve");
      const reserved = await queue.reserve("jobs", 1_000, 2_000);
      expect(reserved?.attempts).toBe(1);
    } finally {
      await queue.close();
    }
  });

  it("propagates the original error when SQLite already rolled back the reserve transaction", async () => {
    const path = tempPath();
    const driver = createSqliteDriver({ path });
    const queue = createQueue(driver);
    const admin = new DatabaseSync(path);
    admins.push(admin);
    try {
      await queue.push("jobs", "body", { availableAt: 1_000, now: 1_000 });
      admin.exec(
        "CREATE TRIGGER rollback_reserve BEFORE UPDATE OF attempts ON messages BEGIN SELECT RAISE(ROLLBACK, 'disk full'); END;",
      );
      await expect(queue.reserve("jobs", 1_000, 2_000)).rejects.toMatchObject({ message: "disk full" });
      admin.exec("DROP TRIGGER rollback_reserve");
      const reserved = await queue.reserve("jobs", 1_000, 2_000);
      expect(reserved?.attempts).toBe(1);
    } finally {
      await queue.close();
    }
  });

  it("throws when used after the driver is closed", async () => {
    const driver = createSqliteDriver({ path: tempPath() });
    await driver.close();
    await expect(driver.push("jobs", "body", { availableAt: 0, now: 0 })).rejects.toThrow();
  });

  it("accepts a push one day ahead and reserves it when due", async () => {
    const queue = createQueue(createSqliteDriver({ path: tempPath() }));
    const DAY = 86_400_000;
    try {
      await queue.push("jobs", "tomorrow", { availableAt: 1_000 + DAY, now: 1_000 });
      expect(await queue.reserve("jobs", 1_000 + DAY - 1, 2_000 + DAY)).toBeNull();
      const reserved = await queue.reserve("jobs", 1_000 + DAY, 2_000 + DAY);
      expect(reserved?.payload).toBe("tomorrow");
    } finally {
      await queue.close();
    }
  });
});
