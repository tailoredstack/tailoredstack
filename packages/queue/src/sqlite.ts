import { DatabaseSync, type StatementSync } from "node:sqlite";
import { InvalidArgumentError } from "./errors.js";
import type { Driver, DriverCapabilities, DriverLease, QueueSize } from "./types.js";

export interface SqliteDriverOptions {
  /** Database file. Several drivers may open the same file; they share one queue. */
  readonly path: string;
  /**
   * Table that holds the messages. Defaults to "messages". Must match /^[A-Za-z_][A-Za-z0-9_]*$/
   * because SQL identifiers cannot be bound parameters. Names are case-insensitive: "Messages" and "messages" are one table.
   * Names that are SQL keywords, start with "sqlite_" or end in "_ready" or "_token" are rejected, because SQLite reserves the first and those index names derive from the table name.
   * Two drivers on one file with different tables are isolated.
   */
  readonly table?: string;
}

export const SQLITE_CAPABILITIES: DriverCapabilities = {
  shared: true,
  exactCounts: true,
  maxPushDelayMs: Number.POSITIVE_INFINITY,
};

const DEFAULT_TABLE = "messages";
const TABLE_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;

const SQLITE_KEYWORDS = new Set(
  "abort action add after all alter always analyze and as asc attach autoincrement before begin between by cascade case cast check collate column commit conflict constraint create cross current current_date current_time current_timestamp database default deferrable deferred delete desc detach distinct do drop each else end escape except exclude exclusive exists explain fail filter first following for foreign from full generated glob group groups having if ignore immediate in index indexed initially inner insert instead intersect into is isnull join key last left like limit match materialized natural no not nothing notnull null nulls of offset on or order others outer over partition plan pragma preceding primary query raise range recursive references regexp reindex release rename replace restrict returning right rollback row rows savepoint select set table temp temporary then ties to transaction trigger unbounded union unique update using vacuum values view virtual when where window with without".split(
    " ",
  ),
);

type ReadyRow = { seq: number; payload: string; attempts: number };
type BucketRow = { bucket: "reserved" | "delayed" | "ready"; count: number };

// `seq` is the push sequence: it orders ties on availableAt and never changes, so release keeps the place.
// A token is "<seq>:<attempts>". Attempts only grow, so every reservation gets a token no earlier one had.
export function createSqliteDriver({ path, table = DEFAULT_TABLE }: SqliteDriverOptions): Driver {
  // Validate before opening the file: the name is interpolated into SQL below.
  if (!TABLE_NAME.test(table)) {
    throw new InvalidArgumentError(
      `invalid sqlite table name ${JSON.stringify(table)}: use letters, digits and underscores, starting with a letter or underscore`,
    );
  }
  const lower = table.toLowerCase();
  if (SQLITE_KEYWORDS.has(lower)) {
    throw new InvalidArgumentError(`invalid sqlite table name ${JSON.stringify(table)}: it is a SQL keyword`);
  }
  if (lower.endsWith("_ready") || lower.endsWith("_token")) {
    throw new InvalidArgumentError(
      `invalid sqlite table name ${JSON.stringify(table)}: names ending in _ready or _token collide with index names`,
    );
  }
  if (lower.startsWith("sqlite_")) {
    throw new InvalidArgumentError(
      `invalid sqlite table name ${JSON.stringify(table)}: the sqlite_ prefix is reserved for internal tables`,
    );
  }
  const readyIndex = `${table}_ready`;
  const tokenIndex = `${table}_token`;

  const db = new DatabaseSync(path);
  db.exec("PRAGMA busy_timeout = 5000"); // wait for other connections' write locks instead of failing
  db.exec(`
    CREATE TABLE IF NOT EXISTS ${table} (
      seq INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      payload TEXT NOT NULL,
      available_at INTEGER NOT NULL,
      attempts INTEGER NOT NULL DEFAULT 0,
      lease_until INTEGER,
      token TEXT
    );
    CREATE INDEX IF NOT EXISTS ${readyIndex} ON ${table} (name, available_at, seq);
    CREATE UNIQUE INDEX IF NOT EXISTS ${tokenIndex} ON ${table} (token);
  `);

  const insert: StatementSync = db.prepare(`INSERT INTO ${table} (name, payload, available_at) VALUES (?, ?, ?)`);
  const ready: StatementSync = db.prepare(
    `SELECT seq, payload, attempts FROM ${table}
     WHERE name = ? AND available_at <= ? AND (lease_until IS NULL OR lease_until <= ?)
     ORDER BY available_at, seq LIMIT 1`,
  );
  const lease: StatementSync = db.prepare(`UPDATE ${table} SET lease_until = ?, token = ?, attempts = ? WHERE seq = ?`);
  const remove: StatementSync = db.prepare(`DELETE FROM ${table} WHERE name = ? AND token = ? AND lease_until > ?`);
  const requeue: StatementSync = db.prepare(
    `UPDATE ${table} SET available_at = ?, lease_until = NULL, token = NULL WHERE name = ? AND token = ? AND lease_until > ?`,
  );
  const buckets: StatementSync = db.prepare(
    `SELECT CASE WHEN lease_until > ? THEN 'reserved' WHEN available_at > ? THEN 'delayed' ELSE 'ready' END AS bucket,
            COUNT(*) AS count
     FROM ${table} WHERE name = ? GROUP BY bucket`,
  );

  return {
    capabilities: SQLITE_CAPABILITIES,

    async push(name, payload, at) {
      return String(insert.run(name, payload, at.availableAt).lastInsertRowid);
    },

    async reserve(name, now, leaseUntil): Promise<DriverLease | null> {
      let result: DriverLease | null = null;
      db.exec("BEGIN IMMEDIATE");
      try {
        const row = ready.get(name, now, now) as ReadyRow | undefined;
        if (row !== undefined) {
          const attempts = row.attempts + 1;
          const token = `${row.seq}:${attempts}`;
          lease.run(leaseUntil, token, attempts, row.seq);
          result = { id: String(row.seq), payload: row.payload, attempts, token };
        }
        db.exec("COMMIT");
      } catch (error) {
        if (db.isTransaction) db.exec("ROLLBACK");
        throw error;
      }
      return result;
    },

    async delete(name, token, now) {
      return Number(remove.run(name, token, now).changes) > 0;
    },

    async release(name, token, availableAt, now) {
      return Number(requeue.run(availableAt, name, token, now).changes) > 0;
    },

    async size(name, now): Promise<QueueSize> {
      const size: QueueSize = { ready: 0, delayed: 0, reserved: 0 };
      for (const row of buckets.all(now, now, name) as BucketRow[]) {
        size[row.bucket] = Number(row.count);
      }
      return size;
    },

    async close() {
      db.close();
    },
  };
}
