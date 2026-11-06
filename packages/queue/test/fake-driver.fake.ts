import type { QueueCounts, QueueDriver, RawJob } from "../src/driver.ts";

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
  readonly #rows: Row[] = [];
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
    if (index < 0 || !this.#holds(this.#rows[index] as Row, attempts, now)) return false;
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

  /** The caller holds the job: same attempt as the stored reservation, and it has not expired. */
  #holds(row: Row, attempts: number, now: number): boolean {
    return row.attempts === attempts && row.reservedUntil !== null && row.reservedUntil > now;
  }
}
