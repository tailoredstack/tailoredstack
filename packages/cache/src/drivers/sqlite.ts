import { DatabaseSync, type StatementSync } from "node:sqlite";
import type { Store } from "../contracts.ts";

export interface SqliteStoreOptions {
  path: string;
  now?: () => number;
}

interface Row {
  value: string;
  expires_at: number | null;
}

export class SqliteStore implements Store {
  readonly #db: DatabaseSync;
  readonly #now: () => number;
  readonly #select: StatementSync;
  readonly #row: StatementSync;
  readonly #upsert: StatementSync;
  readonly #addIgnore: StatementSync;
  readonly #purge: StatementSync;
  readonly #forget: StatementSync;
  readonly #touch: StatementSync;
  readonly #all: StatementSync;
  readonly #forgetIf: StatementSync;
  readonly #extendIf: StatementSync;

  constructor({ path, now = Date.now }: SqliteStoreOptions) {
    this.#db = new DatabaseSync(path);
    this.#db.exec("PRAGMA busy_timeout = 5000"); // wait for other processes' write locks instead of failing
    this.#now = now;
    this.#db.exec("CREATE TABLE IF NOT EXISTS cache (key TEXT PRIMARY KEY, value TEXT NOT NULL, expires_at INTEGER)");
    this.#select = this.#db.prepare("SELECT value FROM cache WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)");
    this.#row = this.#db.prepare("SELECT value, expires_at FROM cache WHERE key = ?");
    this.#upsert = this.#db.prepare(
      "INSERT INTO cache (key, value, expires_at) VALUES (?, ?, ?) " +
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value, expires_at = excluded.expires_at",
    );
    this.#addIgnore = this.#db.prepare("INSERT OR IGNORE INTO cache (key, value, expires_at) VALUES (?, ?, ?)");
    this.#purge = this.#db.prepare("DELETE FROM cache WHERE key = ? AND expires_at <= ?");
    this.#forget = this.#db.prepare("DELETE FROM cache WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)");
    this.#touch = this.#db.prepare(
      "UPDATE cache SET expires_at = ? WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)",
    );
    this.#forgetIf = this.#db.prepare(
      "DELETE FROM cache WHERE key = ? AND value = ? AND (expires_at IS NULL OR expires_at > ?)",
    );
    this.#extendIf = this.#db.prepare(
      "UPDATE cache SET expires_at = ? WHERE key = ? AND value = ? AND (expires_at IS NULL OR expires_at > ?)",
    );
    this.#all = this.#db.prepare("SELECT key, value FROM cache WHERE expires_at IS NULL OR expires_at > ?");
  }

  #expiry(ttlMs: number | null): number | null {
    return ttlMs === null ? null : this.#now() + ttlMs;
  }

  async get(key: string): Promise<unknown> {
    const row = this.#select.get(key, this.#now()) as Pick<Row, "value"> | undefined;
    return row ? JSON.parse(row.value) : undefined;
  }

  async many(keys: string[]): Promise<unknown[]> {
    return Promise.all(keys.map((key) => this.get(key)));
  }

  async put(key: string, value: unknown, ttlMs: number | null): Promise<void> {
    this.#upsert.run(key, JSON.stringify(value), this.#expiry(ttlMs));
  }

  async putMany(values: Record<string, unknown>, ttlMs: number | null): Promise<void> {
    for (const [key, value] of Object.entries(values)) await this.put(key, value, ttlMs);
  }

  async add(key: string, value: unknown, ttlMs: number | null): Promise<boolean> {
    this.#purge.run(key, this.#now());
    return this.#addIgnore.run(key, JSON.stringify(value), this.#expiry(ttlMs)).changes === 1;
  }

  async increment(key: string, by = 1): Promise<number> {
    return this.#adjust(key, by);
  }

  async decrement(key: string, by = 1): Promise<number> {
    return this.#adjust(key, -by);
  }

  // BEGIN IMMEDIATE takes the write lock before the read, so other processes cannot interleave read-modify-write.
  #adjust(key: string, by: number): number {
    this.#db.exec("BEGIN IMMEDIATE");
    try {
      this.#purge.run(key, this.#now());
      const row = this.#row.get(key) as Row | undefined;
      const current: unknown = row ? JSON.parse(row.value) : 0;
      if (!Number.isInteger(current)) throw new TypeError(`Cache value at "${key}" is not an integer`);
      const next = (current as number) + by;
      this.#upsert.run(key, JSON.stringify(next), row?.expires_at ?? null);
      this.#db.exec("COMMIT");
      return next;
    } catch (err) {
      this.#db.exec("ROLLBACK");
      throw err;
    }
  }

  close(): void {
    this.#db.close();
  }

  async touch(key: string, ttlMs: number): Promise<boolean> {
    return this.#touch.run(this.#expiry(ttlMs), key, this.#now()).changes === 1;
  }

  async forget(key: string): Promise<boolean> {
    return this.#forget.run(key, this.#now()).changes === 1;
  }

  async forgetIf(key: string, expected: unknown): Promise<boolean> {
    return this.#forgetIf.run(key, JSON.stringify(expected), this.#now()).changes === 1;
  }

  async extendIf(key: string, expected: unknown, ttlMs: number): Promise<boolean> {
    const now = this.#now();
    return this.#extendIf.run(now + ttlMs, key, JSON.stringify(expected), now).changes === 1;
  }

  async *entries(): AsyncGenerator<{ key: string; value: unknown }> {
    const rows = this.#all.all(this.#now()) as unknown as { key: string; value: string }[];
    for (const row of rows) yield { key: row.key, value: JSON.parse(row.value) };
  }
}
