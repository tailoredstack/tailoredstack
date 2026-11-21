import { randomUUID } from "node:crypto";
import { DatabaseSync, type StatementSync } from "node:sqlite";
import type { Dialect, DialectCapabilities, Message, QueueCount, Receipt } from "./dialect.ts";
import { QueueClosedError } from "./errors.ts";

export interface SqliteDialectOptions {
  path: string;
  /**
   * Table the dialect owns. Defaults to `queue_messages`. Set it to namespace several queues inside one file, for
   * example `table: "billing_queue_messages"`. Its indexes are derived from the name, so two dialects with
   * different tables never collide. Names are quoted, so any non-empty name without a NUL byte is allowed.
   */
  table?: string;
}

const DEFAULT_TABLE = "queue_messages";

// Identifiers cannot be bound as parameters; quote them so a chosen table name cannot break out of the statement.
function identifier(name: string): string {
  if (name === "" || name.includes("\0")) {
    throw new TypeError(`Invalid table name ${JSON.stringify(name)}: must be non-empty and contain no NUL`);
  }
  return `"${name.replaceAll('"', '""')}"`;
}

interface MessageRow {
  readonly id: string;
  readonly payload: string;
  readonly attempts: number;
}

interface CountRow {
  readonly ready: number;
  readonly delayed: number;
  readonly reserved: number;
}

export class SqliteDialect implements Dialect {
  readonly capabilities: DialectCapabilities = {
    ordered: true,
    exactCounts: true,
    clockSource: "caller",
    maxDelayMs: Number.POSITIVE_INFINITY,
    maxLeaseMs: Number.POSITIVE_INFINITY,
  };

  readonly #db: DatabaseSync;
  readonly #insert: StatementSync;
  readonly #next: StatementSync;
  readonly #claim: StatementSync;
  readonly #acknowledge: StatementSync;
  readonly #release: StatementSync;
  readonly #count: StatementSync;
  #closed = false;

  constructor({ path, table = DEFAULT_TABLE }: SqliteDialectOptions) {
    const messages = identifier(table);
    const due = identifier(`${table}_due`);
    const receiptIndex = identifier(`${table}_receipt`);
    const legacyReady = identifier(`${table}_ready`);

    this.#db = new DatabaseSync(path);
    this.#db.exec("PRAGMA busy_timeout = 5000"); // wait for other processes' write locks instead of failing
    this.#db.exec("PRAGMA journal_mode = WAL"); // one fsync per commit instead of two; readers never block writers
    this.#db.exec(
      `CREATE TABLE IF NOT EXISTS ${messages} (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        payload TEXT NOT NULL,
        available_at INTEGER NOT NULL,
        lease_until INTEGER,
        attempts INTEGER NOT NULL DEFAULT 0,
        receipt TEXT
      )`,
    );
    // (name, available_at) lets dequeue read rows already ordered by due time and rowid, with no sort. A receipt
    // index keeps acknowledge and release from scanning every live row of the queue.
    this.#db.exec(`CREATE INDEX IF NOT EXISTS ${due} ON ${messages} (name, available_at)`);
    this.#db.exec(`CREATE INDEX IF NOT EXISTS ${receiptIndex} ON ${messages} (receipt)`);
    this.#db.exec(`DROP INDEX IF EXISTS ${legacyReady}`); // superseded by the due index
    this.#insert = this.#db.prepare(`INSERT INTO ${messages} (id, name, payload, available_at) VALUES (?, ?, ?, ?)`);
    this.#next = this.#db.prepare(
      `SELECT id, payload, attempts FROM ${messages}
       WHERE name = ? AND available_at <= ? AND (lease_until IS NULL OR lease_until <= ?)
       ORDER BY available_at, rowid
       LIMIT 1`,
    );
    this.#claim = this.#db.prepare(
      `UPDATE ${messages} SET lease_until = ?, receipt = ?, attempts = attempts + 1 WHERE id = ?`,
    );
    this.#acknowledge = this.#db.prepare(`DELETE FROM ${messages} WHERE name = ? AND receipt = ? AND lease_until > ?`);
    this.#release = this.#db.prepare(
      `UPDATE ${messages} SET available_at = ?, lease_until = NULL, receipt = NULL
       WHERE name = ? AND receipt = ? AND lease_until > ?`,
    );
    this.#count = this.#db.prepare(
      `SELECT
         COALESCE(SUM(CASE WHEN lease_until > ? THEN 1 ELSE 0 END), 0) AS reserved,
         COALESCE(SUM(CASE WHEN (lease_until IS NULL OR lease_until <= ?) AND available_at <= ? THEN 1 ELSE 0 END), 0) AS ready,
         COALESCE(SUM(CASE WHEN (lease_until IS NULL OR lease_until <= ?) AND available_at > ? THEN 1 ELSE 0 END), 0) AS delayed
       FROM ${messages}
       WHERE name = ?`,
    );
  }

  async enqueue(name: string, payload: string, at: { availableAt: number; now: number }): Promise<string> {
    this.assertOpen();
    const id = randomUUID();
    this.#insert.run(id, name, payload, at.availableAt);
    return id;
  }

  // BEGIN IMMEDIATE takes the write lock before the read, so another process cannot claim the same row between the
  // SELECT and the UPDATE. Under BEGIN DEFERRED both processes read the row first and one fails the upgrade.
  async dequeue(name: string, at: { now: number; leaseUntil: number }): Promise<Message | null> {
    this.assertOpen();
    this.#db.exec("BEGIN IMMEDIATE");
    try {
      const row = this.#next.get(name, at.now, at.now) as unknown as MessageRow | undefined;
      if (!row) {
        this.#db.exec("COMMIT");
        return null;
      }
      const receipt = `${row.id}:${randomUUID()}` as Receipt;
      this.#claim.run(at.leaseUntil, receipt, row.id);
      this.#db.exec("COMMIT");
      return { id: row.id, payload: row.payload, attempts: row.attempts + 1, receipt };
    } catch (err) {
      this.#db.exec("ROLLBACK");
      throw err;
    }
  }

  async acknowledge(name: string, receipt: Receipt, now: number): Promise<boolean> {
    this.assertOpen();
    return this.#acknowledge.run(name, receipt, now).changes === 1;
  }

  async release(name: string, receipt: Receipt, at: { availableAt: number; now: number }): Promise<boolean> {
    this.assertOpen();
    return this.#release.run(at.availableAt, name, receipt, at.now).changes === 1;
  }

  async count(name: string, now: number): Promise<QueueCount> {
    this.assertOpen();
    const row = this.#count.get(now, now, now, now, now, name) as unknown as CountRow;
    return { ready: row.ready, delayed: row.delayed, reserved: row.reserved };
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#db.close();
  }

  private assertOpen(): void {
    if (this.#closed) throw new QueueClosedError("SqliteDialect is closed");
  }
}
