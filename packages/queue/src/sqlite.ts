import { DatabaseSync, type StatementSync } from "node:sqlite";
import type { Driver, DriverCapabilities, DriverLease, QueueSize } from "./types.js";

export interface SqliteDriverOptions {
  /** Database file. Several drivers may open the same file; they share one queue. */
  readonly path: string;
}

export const SQLITE_CAPABILITIES: DriverCapabilities = {
  shared: true,
  exactCounts: true,
  maxPushDelayMs: Number.POSITIVE_INFINITY,
};

type ReadyRow = { seq: number; payload: string; attempts: number };
type BucketRow = { bucket: "reserved" | "delayed" | "ready"; count: number };

// `seq` is the push sequence: it orders ties on availableAt and never changes, so release keeps the place.
// A token is "<seq>:<attempts>". Attempts only grow, so every reservation gets a token no earlier one had.
export function createSqliteDriver({ path }: SqliteDriverOptions): Driver {
  const db = new DatabaseSync(path);
  db.exec("PRAGMA busy_timeout = 5000"); // wait for other connections' write locks instead of failing
  db.exec(`
    CREATE TABLE IF NOT EXISTS messages (
      seq INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      payload TEXT NOT NULL,
      available_at INTEGER NOT NULL,
      attempts INTEGER NOT NULL DEFAULT 0,
      lease_until INTEGER,
      token TEXT
    );
    CREATE INDEX IF NOT EXISTS messages_ready ON messages (name, available_at, seq);
    CREATE UNIQUE INDEX IF NOT EXISTS messages_token ON messages (token);
  `);

  const insert: StatementSync = db.prepare("INSERT INTO messages (name, payload, available_at) VALUES (?, ?, ?)");
  const ready: StatementSync = db.prepare(
    `SELECT seq, payload, attempts FROM messages
     WHERE name = ? AND available_at <= ? AND (lease_until IS NULL OR lease_until <= ?)
     ORDER BY available_at, seq LIMIT 1`,
  );
  const lease: StatementSync = db.prepare("UPDATE messages SET lease_until = ?, token = ?, attempts = ? WHERE seq = ?");
  const remove: StatementSync = db.prepare("DELETE FROM messages WHERE name = ? AND token = ? AND lease_until > ?");
  const requeue: StatementSync = db.prepare(
    "UPDATE messages SET available_at = ?, lease_until = NULL, token = NULL WHERE name = ? AND token = ? AND lease_until > ?",
  );
  const buckets: StatementSync = db.prepare(
    `SELECT CASE WHEN lease_until > ? THEN 'reserved' WHEN available_at > ? THEN 'delayed' ELSE 'ready' END AS bucket,
            COUNT(*) AS count
     FROM messages WHERE name = ? GROUP BY bucket`,
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
        db.exec("ROLLBACK");
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
