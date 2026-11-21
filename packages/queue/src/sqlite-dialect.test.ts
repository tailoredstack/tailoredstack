import { existsSync, mkdtempSync, rmSync } from "node:fs";
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

  it("keeps two dialects on one file apart by table name", async () => {
    const path = join(dir, "shared.db");
    const billing = new SqliteDialect({ path, table: "billing_queue_messages" });
    const reports = new SqliteDialect({ path, table: "reports_queue_messages" });
    opened.push(billing, reports);

    await billing.enqueue("jobs", "invoice", { availableAt: 1000, now: 1000 });
    expect(await reports.count("jobs", 1000)).toEqual({ ready: 0, delayed: 0, reserved: 0 });
    expect(await billing.count("jobs", 1000)).toEqual({ ready: 1, delayed: 0, reserved: 0 });
    expect((await billing.dequeue("jobs", { now: 1000, leaseUntil: 2000 }))?.payload).toBe("invoice");
    expect(await reports.dequeue("jobs", { now: 1000, leaseUntil: 2000 })).toBeNull();

    const probe = new DatabaseSync(path);
    const indexes = probe
      .prepare(
        "SELECT name FROM sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_autoindex%' ORDER BY name",
      )
      .all()
      .map((row) => (row as unknown as { name: string }).name);
    const tables = probe
      .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
      .all()
      .map((row) => (row as unknown as { name: string }).name);
    probe.close();
    expect(tables).toEqual(["billing_queue_messages", "reports_queue_messages"]);
    expect(indexes).toEqual([
      "billing_queue_messages_due",
      "billing_queue_messages_receipt",
      "reports_queue_messages_due",
      "reports_queue_messages_receipt",
    ]);
  });

  it("quotes a table name that needs quoting instead of rejecting it", async () => {
    const path = join(dir, "quoted.db");
    const table = 'billing "queue"; messages';
    const dialect = new SqliteDialect({ path, table });
    opened.push(dialect);

    await dialect.enqueue("jobs", "a", { availableAt: 1000, now: 1000 });
    expect(await dialect.count("jobs", 1000)).toEqual({ ready: 1, delayed: 0, reserved: 0 });

    const probe = new DatabaseSync(path);
    const tables = probe
      .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
      .all()
      .map((row) => (row as unknown as { name: string }).name);
    probe.close();
    expect(tables).toEqual([table]);
  });

  it("rejects an unusable table name before it touches the file", () => {
    const empty = join(dir, "empty-name.db");
    const withNul = join(dir, "nul-name.db");

    expect(() => new SqliteDialect({ path: empty, table: "" })).toThrow(TypeError);
    expect(() => new SqliteDialect({ path: withNul, table: "queue\0messages" })).toThrow(TypeError);
    expect(existsSync(empty)).toBe(false);
    expect(existsSync(withNul)).toBe(false);
  });

  it("rolls back the claim when the read fails inside the transaction", async () => {
    const path = join(dir, "dropped.db");
    const dialect = new SqliteDialect({ path });
    opened.push(dialect);
    await dialect.enqueue("jobs", "a", { availableAt: 1000, now: 1000 });

    const saboteur = new DatabaseSync(path);
    const { sql } = saboteur
      .prepare("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'queue_messages'")
      .get() as unknown as { sql: string };
    saboteur.exec("DROP TABLE queue_messages");
    saboteur.close();

    await expect(dialect.dequeue("jobs", { now: 1000, leaseUntil: 2000 })).rejects.toThrow(/no such table/);

    const repair = new DatabaseSync(path);
    repair.exec(sql);
    repair.close();
    // The rollback released the transaction: this same connection can start a new one.
    await dialect.enqueue("jobs", "b", { availableAt: 1000, now: 1000 });
    await expect(dialect.dequeue("jobs", { now: 1000, leaseUntil: 2000 })).resolves.toMatchObject({ payload: "b" });
  });
});
