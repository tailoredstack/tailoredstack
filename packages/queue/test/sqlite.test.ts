import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it } from "vitest";
import { InvalidArgumentError } from "../src/errors.js";
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

describe("sqlite driver table option", () => {
  const T0 = 1_000;
  const LEASE = 2_000;

  function schemaNames(path: string, type: "table" | "index"): string[] {
    const admin = new DatabaseSync(path);
    admins.push(admin);
    const rows = admin
      .prepare("SELECT name FROM sqlite_master WHERE type = ? AND name NOT LIKE 'sqlite_%' ORDER BY name")
      .all(type) as { name: string }[];
    return rows.map((row) => row.name);
  }

  it("runs push, reserve, release, delete and size end to end on a custom table", async () => {
    const path = tempPath();
    const queue = createQueue(createSqliteDriver({ path, table: "custom_jobs" }));
    try {
      await queue.push("jobs", "first", { availableAt: T0, now: T0 });
      await queue.push("jobs", "second", { availableAt: T0 + 5, now: T0 });
      const stored = new DatabaseSync(path);
      admins.push(stored);
      expect(stored.prepare("SELECT COUNT(*) AS count FROM custom_jobs").get()).toEqual({ count: 2 });
      expect(await queue.size("jobs", T0)).toEqual({ ready: 1, delayed: 1, reserved: 0 });

      const first = await queue.reserve("jobs", T0, LEASE);
      expect(first?.payload).toBe("first");
      expect(await queue.size("jobs", T0)).toEqual({ ready: 0, delayed: 1, reserved: 1 });

      expect(await first?.release(T0, T0 + 1)).toBe(true);
      expect(await queue.size("jobs", T0 + 1)).toEqual({ ready: 1, delayed: 1, reserved: 0 });

      const again = await queue.reserve("jobs", T0 + 1, LEASE);
      expect(again?.payload).toBe("first");
      expect(again?.attempts).toBe(2);
      expect(await again?.delete(T0 + 1)).toBe(true);
      expect(await queue.size("jobs", T0 + 1)).toEqual({ ready: 0, delayed: 1, reserved: 0 });
    } finally {
      await queue.close();
    }
  });

  it("isolates two drivers with different table names on the same file", async () => {
    const path = tempPath();
    const alpha = createQueue(createSqliteDriver({ path, table: "alpha" }));
    const beta = createQueue(createSqliteDriver({ path, table: "beta" }));
    try {
      await alpha.push("jobs", "in-alpha", { availableAt: T0, now: T0 });
      expect(await beta.size("jobs", T0)).toEqual({ ready: 0, delayed: 0, reserved: 0 });
      expect(await beta.reserve("jobs", T0, LEASE)).toBeNull();

      await beta.push("jobs", "in-beta", { availableAt: T0, now: T0 });
      expect((await alpha.reserve("jobs", T0, LEASE))?.payload).toBe("in-alpha");
      expect((await beta.reserve("jobs", T0, LEASE))?.payload).toBe("in-beta");
      expect(await alpha.reserve("jobs", T0, LEASE)).toBeNull();
    } finally {
      await alpha.close();
      await beta.close();
    }
  });

  it("creates the custom table and its indexes, and no default table", async () => {
    const path = tempPath();
    const queue = createQueue(createSqliteDriver({ path, table: "custom_jobs" }));
    try {
      expect(schemaNames(path, "table")).toEqual(["custom_jobs"]);
      expect(schemaNames(path, "index")).toEqual(["custom_jobs_ready", "custom_jobs_token"]);
    } finally {
      await queue.close();
    }
  });

  it("keeps the default table and index names when no table is given", async () => {
    const path = tempPath();
    const queue = createQueue(createSqliteDriver({ path }));
    try {
      expect(schemaNames(path, "table")).toEqual(["messages"]);
      expect(schemaNames(path, "index")).toEqual(["messages_ready", "messages_token"]);
    } finally {
      await queue.close();
    }
  });

  it.each([
    ["empty", ""],
    ["leading digit", "1jobs"],
    ["contains a space", "my jobs"],
    ["contains a double quote", 'jobs"x'],
    ["contains a single quote", "jobs'x"],
    ["contains a semicolon", "jobs;drop"],
    ["contains a hyphen", "my-jobs"],
  ])("rejects a table name that is %s at creation", (_label, table) => {
    const path = tempPath();
    expect(() => createSqliteDriver({ path, table })).toThrow(InvalidArgumentError);
    expect(() => createSqliteDriver({ path, table })).toThrow(/table/);
  });

  it("rejects a table name that collides with the default index names on a file the default driver uses", async () => {
    const path = tempPath();
    const queue = createQueue(createSqliteDriver({ path }));
    try {
      expect(() => createSqliteDriver({ path, table: "messages_ready" })).toThrow(InvalidArgumentError);
      expect(() => createSqliteDriver({ path, table: "messages_ready" })).toThrow(/table/);
      expect(schemaNames(path, "index")).toEqual(["messages_ready", "messages_token"]);
    } finally {
      await queue.close();
    }
  });

  it.each([
    ["a default ready index name", "messages_ready"],
    ["a default token index name", "messages_token"],
    ["a token index name of another table", "jobs_token"],
    ["an upper-case ready index name", "JOBS_READY"],
  ])("rejects a table name that is %s before opening the file", (_label, table) => {
    const path = tempPath();
    expect(() => createSqliteDriver({ path, table })).toThrow(InvalidArgumentError);
    expect(existsSync(path)).toBe(false);
  });

  it.each([
    ["lower case", "select"],
    ["upper case", "SELECT"],
    ["mixed case", "Table"],
    ["a keyword used as a table", "where"],
  ])("rejects a SQL keyword as a table name (%s) before opening the file", (_label, table) => {
    const path = tempPath();
    expect(() => createSqliteDriver({ path, table })).toThrow(InvalidArgumentError);
    expect(existsSync(path)).toBe(false);
  });

  it("treats table names case-insensitively, so Messages is the default queue", async () => {
    const path = tempPath();
    const upper = createQueue(createSqliteDriver({ path, table: "Messages" }));
    const lower = createQueue(createSqliteDriver({ path }));
    try {
      await upper.push("jobs", "shared", { availableAt: T0, now: T0 });
      expect(await lower.size("jobs", T0)).toEqual({ ready: 1, delayed: 0, reserved: 0 });
      expect((await lower.reserve("jobs", T0, LEASE))?.payload).toBe("shared");
      expect(schemaNames(path, "table")).toEqual(["Messages"]);
    } finally {
      await upper.close();
      await lower.close();
    }
  });
});
