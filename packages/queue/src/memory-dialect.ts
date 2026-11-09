import type { Dialect, DialectCapabilities, Message, QueueCount, Receipt } from "./dialect.ts";

interface StoredMessage {
  readonly id: string;
  readonly name: string;
  readonly payload: string;
  readonly availableAt: number;
  leaseUntil: number;
  attempts: number;
}

/**
 * In-process `Dialect`. Never reads a clock: every time is the caller's `now`.
 * `acknowledge`, `release`, and `close` are pending work.
 */
export class MemoryDialect implements Dialect {
  readonly capabilities: DialectCapabilities = {
    ordered: true,
    exactCounts: true,
    clockSource: "caller",
    maxDelayMs: Number.POSITIVE_INFINITY,
    maxLeaseMs: Number.POSITIVE_INFINITY,
  };

  private readonly messages: StoredMessage[] = [];
  private sequence = 0;
  private receiptSequence = 0;

  async enqueue(name: string, payload: string, at: { availableAt: number; now: number }): Promise<string> {
    this.sequence += 1;
    const id = `memory-${this.sequence}`;
    this.messages.push({
      id,
      name,
      payload,
      availableAt: at.availableAt,
      leaseUntil: Number.NEGATIVE_INFINITY,
      attempts: 0,
    });
    return id;
  }

  async dequeue(name: string, at: { now: number; leaseUntil: number }): Promise<Message | null> {
    // Ready: due and not under a live lease (live means now < leaseUntil). Earliest-due wins;
    // the stable sort keeps push order for ties.
    const [next] = this.messages
      .filter((m) => m.name === name && m.availableAt <= at.now && m.leaseUntil <= at.now)
      .sort((a, b) => a.availableAt - b.availableAt);
    if (!next) return null;
    next.leaseUntil = at.leaseUntil;
    next.attempts += 1;
    this.receiptSequence += 1;
    return {
      id: next.id,
      payload: next.payload,
      attempts: next.attempts,
      receipt: `${next.id}:${this.receiptSequence}` as Receipt,
    };
  }

  async acknowledge(_name: string, _receipt: Receipt, _now: number): Promise<boolean> {
    throw new Error("not implemented");
  }

  async release(_name: string, _receipt: Receipt, _at: { availableAt: number; now: number }): Promise<boolean> {
    throw new Error("not implemented");
  }

  async count(name: string, now: number): Promise<QueueCount> {
    let ready = 0;
    let delayed = 0;
    let reserved = 0;
    for (const message of this.messages) {
      if (message.name !== name) continue;
      if (message.leaseUntil > now) {
        reserved += 1;
      } else if (message.availableAt <= now) {
        ready += 1;
      } else {
        delayed += 1;
      }
    }
    return { ready, delayed, reserved };
  }

  async close(): Promise<void> {
    throw new Error("not implemented");
  }
}
