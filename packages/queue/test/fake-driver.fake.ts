import type { QueueCounts, QueueDriver, RawJob } from "../src/driver.ts";

interface Row {
  readonly id: string;
  readonly queue: string;
  readonly payload: string;
  readonly availableAt: number;
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
}
