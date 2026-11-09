import type { Dialect, DialectCapabilities, Message, QueueCount, Receipt } from "./dialect.ts";

interface StoredMessage {
  readonly id: string;
  readonly name: string;
  readonly payload: string;
  availableAt: number;
  leaseUntil: number;
  attempts: number;
  /** Receipt of the current delivery; null when the message is not reserved. */
  receipt: Receipt | null;
}

/**
 * In-process `Dialect`. Never reads a clock: every time is the caller's `now`.
 * After `close`, every other method rejects.
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
  private closed = false;

  async enqueue(name: string, payload: string, at: { availableAt: number; now: number }): Promise<string> {
    this.assertOpen();
    this.sequence += 1;
    const id = `memory-${this.sequence}`;
    this.messages.push({
      id,
      name,
      payload,
      availableAt: at.availableAt,
      leaseUntil: Number.NEGATIVE_INFINITY,
      attempts: 0,
      receipt: null,
    });
    return id;
  }

  async dequeue(name: string, at: { now: number; leaseUntil: number }): Promise<Message | null> {
    this.assertOpen();
    // Ready: due and not under a live lease (live means now < leaseUntil). Earliest-due wins;
    // the stable sort keeps push order for ties.
    const [next] = this.messages
      .filter((m) => m.name === name && m.availableAt <= at.now && m.leaseUntil <= at.now)
      .sort((a, b) => a.availableAt - b.availableAt);
    if (!next) return null;
    next.leaseUntil = at.leaseUntil;
    next.attempts += 1;
    this.receiptSequence += 1;
    const receipt = `${next.id}:${this.receiptSequence}` as Receipt;
    next.receipt = receipt;
    return { id: next.id, payload: next.payload, attempts: next.attempts, receipt };
  }

  async acknowledge(name: string, receipt: Receipt, now: number): Promise<boolean> {
    this.assertOpen();
    const message = this.findLive(name, receipt, now);
    if (!message) return false;
    this.messages.splice(this.messages.indexOf(message), 1);
    return true;
  }

  async release(name: string, receipt: Receipt, at: { availableAt: number; now: number }): Promise<boolean> {
    this.assertOpen();
    const message = this.findLive(name, receipt, at.now);
    if (!message) return false;
    message.leaseUntil = Number.NEGATIVE_INFINITY;
    message.availableAt = at.availableAt;
    message.receipt = null;
    return true;
  }

  async count(name: string, now: number): Promise<QueueCount> {
    this.assertOpen();
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

  /** The message holding `receipt` on queue `name` if that receipt is current and its lease is live. */
  private findLive(name: string, receipt: Receipt, now: number): StoredMessage | undefined {
    return this.messages.find((m) => m.name === name && m.receipt === receipt && m.leaseUntil > now);
  }

  async close(): Promise<void> {
    this.closed = true;
  }

  private assertOpen(): void {
    if (this.closed) throw new Error("MemoryDialect is closed");
  }
}
