import { DatabaseSync, type StatementSync } from "node:sqlite";
import type { QueueCounts, QueueDriver, RawFailedJob, RawJob } from "../driver.ts";

export interface SqliteQueueDriverOptions {
  path: string;
}

interface JobRow {
  id: number;
  queue: string;
  payload: string;
  attempts: number;
}

interface FailedRow {
  id: number;
  queue: string;
  payload: string;
  attempts: number;
  error: string;
  failed_at: number;
}

// Own tables, separate from the cache's key-value table. `reserved_until` is the lease end; `reserved_at` is when the
// current lease started. Both are NULL while the job is ready. Times are epoch ms supplied by the core.
const SCHEMA = `
  CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue TEXT NOT NULL,
    payload TEXT NOT NULL,
    available_at INTEGER NOT NULL,
    reserved_at INTEGER,
    reserved_until INTEGER,
    attempts INTEGER NOT NULL DEFAULT 0
  );
  CREATE INDEX IF NOT EXISTS jobs_queue_available ON jobs (queue, available_at, id);
  CREATE TABLE IF NOT EXISTS failed_jobs (
    id INTEGER PRIMARY KEY,
    queue TEXT NOT NULL,
    payload TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    error TEXT NOT NULL,
    failed_at INTEGER NOT NULL
  );
`;

// Lease check shared by remove, release, and fail: the caller's attempt count matches and the lease is unexpired.
const HELD = "id = ? AND queue = ? AND attempts = ? AND reserved_until > ?";

export class SqliteQueueDriver implements QueueDriver {
  readonly #db: DatabaseSync;
  readonly #insert: StatementSync;
  readonly #pick: StatementSync;
  readonly #claim: StatementSync;
  readonly #remove: StatementSync;
  readonly #release: StatementSync;
  readonly #held: StatementSync;
  readonly #deleteJob: StatementSync;
  readonly #insertFailed: StatementSync;
  readonly #requeue: StatementSync;
  readonly #failedOne: StatementSync;
  readonly #failedFor: StatementSync;
  readonly #deleteFailedFor: StatementSync;
  readonly #forgetFailed: StatementSync;
  readonly #retryAll: StatementSync;
  readonly #pruneFailed: StatementSync;
  readonly #counts: StatementSync;

  constructor({ path }: SqliteQueueDriverOptions) {
    this.#db = new DatabaseSync(path);
    this.#db.exec("PRAGMA busy_timeout = 5000"); // wait for other connections' write locks instead of failing
    this.#db.exec(SCHEMA);
    this.#insert = this.#db.prepare("INSERT INTO jobs (queue, payload, available_at) VALUES (?, ?, ?)");
    this.#pick = this.#db.prepare(
      "SELECT id, queue, payload, attempts FROM jobs WHERE queue = ? AND available_at <= ? " +
        "AND (reserved_until IS NULL OR reserved_until <= ?) ORDER BY available_at, id LIMIT 1",
    );
    this.#claim = this.#db.prepare(
      "UPDATE jobs SET reserved_at = ?, reserved_until = ?, attempts = attempts + 1 WHERE id = ? RETURNING attempts",
    );
    this.#remove = this.#db.prepare(`DELETE FROM jobs WHERE ${HELD}`);
    this.#release = this.#db.prepare(
      `UPDATE jobs SET available_at = ?, reserved_at = NULL, reserved_until = NULL WHERE ${HELD}`,
    );
    this.#held = this.#db.prepare(`SELECT id, queue, payload, attempts FROM jobs WHERE ${HELD}`);
    this.#deleteJob = this.#db.prepare("DELETE FROM jobs WHERE id = ?");
    this.#insertFailed = this.#db.prepare(
      "INSERT INTO failed_jobs (id, queue, payload, attempts, error, failed_at) VALUES (?, ?, ?, ?, ?, ?)",
    );
    this.#requeue = this.#db.prepare(
      "INSERT INTO jobs (id, queue, payload, available_at, attempts) VALUES (?, ?, ?, ?, 0)",
    );
    this.#failedOne = this.#db.prepare("SELECT id, queue, payload FROM failed_jobs WHERE id = ?");
    this.#failedFor = this.#db.prepare(
      "SELECT id, queue, payload, attempts, error, failed_at FROM failed_jobs WHERE ? IS NULL OR queue = ? " +
        "ORDER BY failed_at, id",
    );
    this.#forgetFailed = this.#db.prepare("DELETE FROM failed_jobs WHERE id = ?");
    this.#retryAll = this.#db.prepare(
      "INSERT INTO jobs (id, queue, payload, available_at, attempts) " +
        "SELECT id, queue, payload, ?, 0 FROM failed_jobs WHERE ? IS NULL OR queue = ?",
    );
    this.#deleteFailedFor = this.#db.prepare("DELETE FROM failed_jobs WHERE ? IS NULL OR queue = ?");
    this.#pruneFailed = this.#db.prepare("DELETE FROM failed_jobs WHERE failed_at < ?");
    this.#counts = this.#db.prepare(
      "SELECT CASE WHEN reserved_until > ? THEN 'reserved' WHEN available_at > ? THEN 'delayed' ELSE 'pending' END " +
        "AS state, COUNT(*) AS n FROM jobs WHERE queue = ? GROUP BY state",
    );
  }

  // One write transaction. BEGIN IMMEDIATE takes the write lock up front, so a read-then-write cannot interleave.
  #transaction<T>(work: () => T): T {
    this.#db.exec("BEGIN IMMEDIATE");
    try {
      const result = work();
      this.#db.exec("COMMIT");
      return result;
    } catch (error) {
      this.#db.exec("ROLLBACK");
      throw error;
    }
  }

  async insert(queue: string, payload: string, availableAt: number): Promise<string> {
    return String(this.#insert.run(queue, payload, availableAt).lastInsertRowid);
  }

  async reserve(queue: string, now: number, reservedUntil: number): Promise<RawJob | null> {
    return this.#transaction(() => {
      const row = this.#pick.get(queue, now, now) as JobRow | undefined;
      if (!row) return null;
      const { attempts } = this.#claim.get(now, reservedUntil, row.id) as { attempts: number };
      return { id: String(row.id), queue: row.queue, payload: row.payload, attempts };
    });
  }

  async remove(queue: string, id: string, attempts: number, now: number): Promise<boolean> {
    return Number(this.#remove.run(id, queue, attempts, now).changes) > 0;
  }

  async release(queue: string, id: string, attempts: number, now: number, availableAt: number): Promise<boolean> {
    return Number(this.#release.run(availableAt, id, queue, attempts, now).changes) > 0;
  }

  async fail(queue: string, id: string, attempts: number, now: number, error: string): Promise<boolean> {
    return this.#transaction(() => {
      const row = this.#held.get(id, queue, attempts, now) as JobRow | undefined;
      if (!row) return false;
      this.#insertFailed.run(row.id, row.queue, row.payload, row.attempts, error, now);
      this.#deleteJob.run(row.id);
      return true;
    });
  }

  async failedJobs(queue: string | null): Promise<RawFailedJob[]> {
    const rows = this.#failedFor.all(queue, queue) as unknown as FailedRow[];
    return rows.map(toFailed);
  }

  async retryFailed(id: string, now: number): Promise<boolean> {
    return this.#transaction(() => {
      const row = this.#failedOne.get(id) as { id: number; queue: string; payload: string } | undefined;
      if (!row) return false;
      this.#forgetFailed.run(row.id);
      this.#requeue.run(row.id, row.queue, row.payload, now);
      return true;
    });
  }

  async retryAllFailed(queue: string | null, now: number): Promise<number> {
    return this.#transaction(() => {
      const moved = Number(this.#retryAll.run(now, queue, queue).changes);
      this.#deleteFailedFor.run(queue, queue);
      return moved;
    });
  }

  async forgetFailed(id: string): Promise<boolean> {
    return Number(this.#forgetFailed.run(id).changes) > 0;
  }

  async flushFailed(queue: string | null): Promise<number> {
    return Number(this.#deleteFailedFor.run(queue, queue).changes);
  }

  async pruneFailed(cutoff: number): Promise<number> {
    return Number(this.#pruneFailed.run(cutoff).changes);
  }

  async counts(queue: string, now: number): Promise<QueueCounts> {
    const counts = { pending: 0, delayed: 0, reserved: 0 };
    const rows = this.#counts.all(now, now, queue) as unknown as { state: keyof QueueCounts; n: number }[];
    for (const row of rows) counts[row.state] = Number(row.n);
    return counts;
  }
}

function toFailed(row: FailedRow): RawFailedJob {
  return {
    id: String(row.id),
    queue: row.queue,
    payload: row.payload,
    attempts: row.attempts,
    error: row.error,
    failedAt: row.failed_at,
  };
}
