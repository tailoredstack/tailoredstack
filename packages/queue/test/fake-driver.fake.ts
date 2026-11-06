import type { QueueCounts, QueueDriver, RawFailedJob, RawJob } from "../src/driver.ts";

interface Row {
  readonly id: string;
  readonly queue: string;
  readonly payload: string;
  availableAt: number;
  reservedUntil: number | null;
  attempts: number;
}

/** In-memory driver for tests only. Not exported from the package. */
export class FakeDriver implements QueueDriver {
  #rows: Row[] = [];
  #failed: RawFailedJob[] = [];
  #seq = 0;

  async insert(queue: string, payload: string, availableAt: number): Promise<string> {
    const id = String(++this.#seq);
    this.#rows.push({ id, queue, payload, availableAt, reservedUntil: null, attempts: 0 });
    return id;
  }

  async reserve(queue: string, now: number, reservedUntil: number): Promise<RawJob | null> {
    const row = this.#rows.find(
      (r) => r.queue === queue && r.availableAt <= now && (r.reservedUntil === null || r.reservedUntil <= now),
    );
    if (!row) return null;
    row.reservedUntil = reservedUntil;
    row.attempts += 1;
    return { id: row.id, queue: row.queue, payload: row.payload, attempts: row.attempts };
  }

  async remove(queue: string, id: string, attempts: number, now: number): Promise<boolean> {
    const index = this.#rows.findIndex((r) => r.id === id && r.queue === queue);
    const row = this.#rows[index];
    if (!row || !this.#holds(row, attempts, now)) return false;
    this.#rows.splice(index, 1);
    return true;
  }

  async release(queue: string, id: string, attempts: number, now: number, availableAt: number): Promise<boolean> {
    const row = this.#rows.find((r) => r.id === id && r.queue === queue);
    if (!row || !this.#holds(row, attempts, now)) return false;
    row.reservedUntil = null;
    row.availableAt = availableAt;
    return true;
  }

  async fail(queue: string, id: string, attempts: number, now: number, error: string): Promise<boolean> {
    const index = this.#rows.findIndex((r) => r.id === id && r.queue === queue);
    const row = this.#rows[index];
    if (!row || !this.#holds(row, attempts, now)) return false;
    this.#rows.splice(index, 1);
    this.#failed.push({
      id: row.id,
      queue: row.queue,
      payload: row.payload,
      attempts: row.attempts,
      error,
      failedAt: now,
    });
    return true;
  }

  async failedJobs(queue: string | null): Promise<RawFailedJob[]> {
    return this.#failed.filter((f) => queue === null || f.queue === queue).map((f) => ({ ...f }));
  }

  async retryFailed(id: string, now: number): Promise<boolean> {
    const index = this.#failed.findIndex((f) => f.id === id);
    const record = this.#failed[index];
    if (!record) return false;
    this.#failed.splice(index, 1);
    this.#requeue(record, now);
    return true;
  }

  async retryAllFailed(queue: string | null, now: number): Promise<number> {
    const matching = this.#failed.filter((f) => queue === null || f.queue === queue);
    for (const record of matching) this.#requeue(record, now);
    this.#failed = this.#failed.filter((f) => !matching.includes(f));
    return matching.length;
  }

  async forgetFailed(id: string): Promise<boolean> {
    const index = this.#failed.findIndex((f) => f.id === id);
    if (index < 0) return false;
    this.#failed.splice(index, 1);
    return true;
  }

  async flushFailed(queue: string | null): Promise<number> {
    const before = this.#failed.length;
    this.#failed = this.#failed.filter((f) => !(queue === null || f.queue === queue));
    return before - this.#failed.length;
  }

  async pruneFailed(cutoff: number): Promise<number> {
    const before = this.#failed.length;
    this.#failed = this.#failed.filter((f) => f.failedAt >= cutoff);
    return before - this.#failed.length;
  }

  async counts(queue: string, now: number): Promise<QueueCounts> {
    let pending = 0;
    let delayed = 0;
    let reserved = 0;
    for (const r of this.#rows) {
      if (r.queue !== queue) continue;
      if (r.reservedUntil !== null && r.reservedUntil > now) reserved += 1;
      else if (r.availableAt > now) delayed += 1;
      else pending += 1;
    }
    return { pending, delayed, reserved };
  }

  #requeue(record: RawFailedJob, now: number): void {
    this.#rows.push({
      id: record.id,
      queue: record.queue,
      payload: record.payload,
      availableAt: now,
      reservedUntil: null,
      attempts: 0,
    });
  }

  /** The caller holds the job: same attempt as the stored reservation, and it has not expired. */
  #holds(row: Row, attempts: number, now: number): boolean {
    return row.attempts === attempts && row.reservedUntil !== null && row.reservedUntil > now;
  }
}
