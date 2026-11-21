import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterAll, afterEach, describe, expect, it } from "vitest";
import { runDialectSuite } from "./dialect-suite.ts";
import { SqliteDialect } from "./sqlite-dialect.ts";

const dir = mkdtempSync(join(tmpdir(), "queue-sqlite-dialect-"));
const opened: SqliteDialect[] = [];
let files = 0;

const open = (): SqliteDialect => {
  const dialect = new SqliteDialect({ path: join(dir, `queue-${files++}.db`) });
  opened.push(dialect);
  return dialect;
};

const closeAll = async (): Promise<void> => {
  for (const dialect of opened.splice(0)) await dialect.close();
};

describe("SqliteDialect", () => {
  afterEach(closeAll);
  afterAll(async () => {
    await closeAll();
    rmSync(dir, { recursive: true, force: true });
  });

  runDialectSuite("SqliteDialect", open);

  it("declares ordered, exact counts, a caller clock, and no delay or lease caps", () => {
    expect(open().capabilities).toEqual({
      ordered: true,
      exactCounts: true,
      clockSource: "caller",
      maxDelayMs: Number.POSITIVE_INFINITY,
      maxLeaseMs: Number.POSITIVE_INFINITY,
    });
  });

  it("stores messages in a real file that a second instance can read", async () => {
    const path = join(dir, "persisted.db");
    const first = new SqliteDialect({ path });
    const id = await first.enqueue("jobs", "a", { availableAt: 1000, now: 1000 });
    await first.close();

    const second = new SqliteDialect({ path });
    opened.push(second);
    const message = await second.dequeue("jobs", { now: 1000, leaseUntil: 2000 });
    expect(message?.id).toBe(id);
  });

  it("rolls back the claim when the read fails inside the transaction", async () => {
    const path = join(dir, "dropped.db");
    const dialect = new SqliteDialect({ path });
    opened.push(dialect);
    await dialect.enqueue("jobs", "a", { availableAt: 1000, now: 1000 });

    const saboteur = new DatabaseSync(path);
    saboteur.exec("DROP TABLE queue_messages");
    saboteur.close();

    await expect(dialect.dequeue("jobs", { now: 1000, leaseUntil: 2000 })).rejects.toThrow(/no such table/);
  });
});
