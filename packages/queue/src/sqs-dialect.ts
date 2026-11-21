import { createHash } from "node:crypto";
import type { SQSClient } from "@aws-sdk/client-sqs";
import type { Dialect, DialectCapabilities, Message, QueueCount, Receipt } from "./dialect.ts";

export type SqsClient = Pick<SQSClient, "send">;

export interface SqsDialectOptions {
  /** The caller owns the client; the dialect never destroys it. */
  readonly client: SqsClient;
  /**
   * Prefix of the SQS queue name the dialect creates for a queue name. The dialect appends a hyphen and 32 hex
   * characters, so the prefix may be at most 47 characters. Defaults to `queue`.
   */
  readonly queueNamePrefix?: string;
}

// The SDK is an optional peer. Import it on first use so a MemoryDialect or SqliteDialect user never loads it.
type SqsSdk = typeof import("@aws-sdk/client-sqs");

// The runtime caches the module, so importing again is cheap, and a failed import is retried on the next call
// instead of becoming a permanently cached rejection.
function sqs(): Promise<SqsSdk> {
  return import("@aws-sdk/client-sqs");
}

const SECOND_MS = 1_000;
const MAX_DELAY_SECONDS = 900;
const MAX_LEASE_SECONDS = 43_200;
// Acknowledged tombstones are bounded. Dropping the oldest only lets a very late retry of an already-deleted message
// return true; it can never delete a live message.
const ACKNOWLEDGED_LIMIT = 1_024;
// SQS keeps a message for at most 14 days. Keep a margin past that window before forgetting a settled id, so the
// message it referred to can no longer be redelivered.
const RETAINED_MS = 15 * 24 * 60 * 60 * 1_000;
const SWEEP_EVERY = 1_024;
const STALE_RECEIPT_CODES: Record<string, true> = {
  ReceiptHandleIsInvalid: true,
  MessageNotInflight: true,
};

function isStaleReceipt(error: unknown): boolean {
  return error instanceof Error && STALE_RECEIPT_CODES[error.name] === true;
}

function secondsUntil(at: number, now: number, max: number): number {
  return Math.min(max, Math.max(0, Math.ceil((at - now) / SECOND_MS)));
}

function parseCount(value: string | undefined): number {
  return value === undefined ? 0 : Number.parseInt(value, 10);
}

// The receipt keeps the SQS handle and the message id together. The id is what lets a dialect instance recognize a
// receipt from an earlier delivery it made itself, without trusting SQS to reject the stale handle.
function splitReceipt(receipt: Receipt): { id: string; handle: string } | null {
  const separator = receipt.indexOf(":");
  if (separator <= 0 || separator === receipt.length - 1) return null;
  return { id: receipt.slice(0, separator), handle: receipt.slice(separator + 1) };
}

type MessageStatus = "leased" | "settling" | "released" | "acknowledged" | "expired";

interface MessageState {
  readonly receipt: Receipt;
  readonly leaseUntil: number;
  readonly settledAt: number;
  readonly status: MessageStatus;
}

export const SQS_CAPABILITIES: DialectCapabilities = Object.freeze({
  ordered: false,
  exactCounts: false,
  clockSource: "backend",
  maxDelayMs: MAX_DELAY_SECONDS * SECOND_MS,
  maxLeaseMs: MAX_LEASE_SECONDS * SECOND_MS,
});

export class SqsDialect implements Dialect {
  readonly capabilities: DialectCapabilities = SQS_CAPABILITIES;

  readonly #client: SqsClient;
  readonly #prefix: string;
  readonly #urls = new Map<string, Promise<string>>();
  // One state per message id, not per delivery. A receipt that is not the latest for its id is stale, so no receipt
  // from an earlier delivery can ever be treated as unknown and passed to SQS, which would delete a live message.
  readonly #states = new Map<string, MessageState>();
  // Ids whose latest receipt was acknowledged. Only these tombstones are bounded: forgetting one is safe.
  readonly #acknowledged = new Map<string, Receipt>();
  #marks = 0;
  #closed = false;

  constructor({ client, queueNamePrefix = "queue" }: SqsDialectOptions) {
    if (!/^[A-Za-z0-9_-]{1,47}$/.test(queueNamePrefix)) {
      throw new TypeError(
        `Invalid SQS queue name prefix ${JSON.stringify(queueNamePrefix)}: use 1-47 letters, digits, hyphens, or underscores`,
      );
    }
    this.#client = client;
    this.#prefix = queueNamePrefix;
  }

  async enqueue(name: string, payload: string, at: { availableAt: number; now: number }): Promise<string> {
    this.#assertOpen();
    const { SendMessageCommand } = await sqs();
    const url = await this.#queueUrl(name);
    const reply = await this.#client.send(
      new SendMessageCommand({
        QueueUrl: url,
        MessageBody: payload,
        DelaySeconds: secondsUntil(at.availableAt, Date.now(), MAX_DELAY_SECONDS),
      }),
    );
    if (!reply.MessageId) throw new Error(`SQS did not return a message id for queue ${name}`);
    return reply.MessageId;
  }

  async dequeue(name: string, at: { now: number; leaseUntil: number }): Promise<Message | null> {
    this.#assertOpen();
    const { ReceiveMessageCommand } = await sqs();
    const url = await this.#queueUrl(name);
    const now = Date.now();
    const leaseSeconds = secondsUntil(at.leaseUntil, now, MAX_LEASE_SECONDS);
    const reply = await this.#client.send(
      new ReceiveMessageCommand({
        QueueUrl: url,
        MaxNumberOfMessages: 1,
        VisibilityTimeout: leaseSeconds,
        MessageSystemAttributeNames: ["ApproximateReceiveCount"],
        WaitTimeSeconds: 0,
      }),
    );
    const [message] = reply.Messages ?? [];
    if (message === undefined) return null;
    const id = message.MessageId;
    const handle = message.ReceiptHandle;
    if (id === undefined || handle === undefined) {
      throw new Error(`SQS returned a message without an id or receipt handle on queue ${name}`);
    }
    if (message.Body === undefined) throw new Error(`SQS returned a message without a body on queue ${name}`);
    const receipt = `${id}:${handle}` as Receipt;
    // The backend owns the lease: record the visibility SQS actually granted, not the caller's timestamp.
    this.#lease(id, receipt, now + leaseSeconds * SECOND_MS);
    return {
      id,
      payload: message.Body,
      attempts: Number(message.Attributes?.ApproximateReceiveCount ?? "1"),
      receipt,
    };
  }

  async acknowledge(name: string, receipt: Receipt, _now: number): Promise<boolean> {
    this.#assertOpen();
    const split = splitReceipt(receipt);
    if (split === null) return false;
    if (this.#isSettled(split.id, receipt, Date.now())) return false;
    const { DeleteMessageCommand } = await sqs();
    const url = await this.#queueUrl(name);
    // Queue creation and the cache miss can take long enough for the lease to expire after the first check.
    if (this.#isSettled(split.id, receipt, Date.now())) return false;
    const previous = this.#states.get(split.id);
    if (!this.#claim(split.id, receipt, Date.now())) return false;
    try {
      await this.#client.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: split.handle }));
    } catch (error) {
      if (!isStaleReceipt(error)) {
        this.#restore(split.id, receipt, previous);
        throw error;
      }
      this.#mark(split.id, receipt, "acknowledged");
      return false;
    }
    this.#mark(split.id, receipt, "acknowledged");
    return true;
  }

  async release(name: string, receipt: Receipt, at: { availableAt: number; now: number }): Promise<boolean> {
    this.#assertOpen();
    const split = splitReceipt(receipt);
    if (split === null) return false;
    if (this.#isSettled(split.id, receipt, Date.now())) return false;
    const { ChangeMessageVisibilityCommand } = await sqs();
    const url = await this.#queueUrl(name);
    if (this.#isSettled(split.id, receipt, Date.now())) return false;
    const previous = this.#states.get(split.id);
    if (!this.#claim(split.id, receipt, Date.now())) return false;
    try {
      await this.#client.send(
        new ChangeMessageVisibilityCommand({
          QueueUrl: url,
          ReceiptHandle: split.handle,
          VisibilityTimeout: secondsUntil(at.availableAt, Date.now(), MAX_DELAY_SECONDS),
        }),
      );
    } catch (error) {
      if (!isStaleReceipt(error)) {
        this.#restore(split.id, receipt, previous);
        throw error;
      }
      this.#mark(split.id, receipt, "released");
      return false;
    }
    this.#mark(split.id, receipt, "released");
    return true;
  }

  async count(name: string, _now: number): Promise<QueueCount> {
    this.#assertOpen();
    const { GetQueueAttributesCommand } = await sqs();
    const url = await this.#queueUrl(name);
    const reply = await this.#client.send(
      new GetQueueAttributesCommand({
        QueueUrl: url,
        AttributeNames: [
          "ApproximateNumberOfMessages",
          "ApproximateNumberOfMessagesDelayed",
          "ApproximateNumberOfMessagesNotVisible",
        ],
      }),
    );
    const attributes = reply.Attributes ?? {};
    // SQS cannot tell an in-flight message from one released with a visibility delay, so both are reported as
    // reserved rather than delayed. `exactCounts: false` covers the approximation.
    return {
      ready: parseCount(attributes.ApproximateNumberOfMessages),
      delayed: parseCount(attributes.ApproximateNumberOfMessagesDelayed),
      reserved: parseCount(attributes.ApproximateNumberOfMessagesNotVisible),
    };
  }

  async close(): Promise<void> {
    this.#closed = true;
  }

  #lease(id: string, receipt: Receipt, leaseUntil: number): void {
    this.#acknowledged.delete(id);
    this.#states.set(id, { receipt, leaseUntil, settledAt: 0, status: "leased" });
  }

  // Claiming happens synchronously, after the last check and before the first await, so a second call with the same
  // receipt sees "settling" and returns false instead of sending a second delete or visibility change.
  #claim(id: string, receipt: Receipt, now: number): boolean {
    const state = this.#states.get(id);
    if (state === undefined) {
      this.#states.set(id, { receipt, leaseUntil: 0, settledAt: now, status: "settling" });
      return true;
    }
    if (state.leaseUntil <= now) {
      this.#mark(id, receipt, "expired");
      return false;
    }
    this.#states.set(id, { ...state, receipt, settledAt: now, status: "settling" });
    return true;
  }

  // Only undo a claim that still owns the id. A redelivery can replace the settling state while the SQS call is in
  // flight; restoring the old lease then would make a stale receipt look live again.
  #restore(id: string, receipt: Receipt, previous: MessageState | undefined): void {
    if (this.#states.get(id)?.receipt !== receipt) return;
    if (previous === undefined) this.#states.delete(id);
    else this.#states.set(id, previous);
  }

  #mark(id: string, receipt: Receipt, status: Exclude<MessageStatus, "leased" | "settling">): void {
    // Same rule as #restore: never overwrite a newer delivery that arrived while the call was in flight.
    if (this.#states.get(id)?.receipt !== receipt) return;
    const now = Date.now();
    this.#states.set(id, { receipt, leaseUntil: 0, settledAt: now, status });
    if (status === "acknowledged") {
      this.#acknowledged.set(id, receipt);
      if (this.#acknowledged.size > ACKNOWLEDGED_LIMIT) {
        for (const oldestId of this.#acknowledged.keys()) {
          this.#acknowledged.delete(oldestId);
          // An id leaves #acknowledged before its state can be leased or released again, so this deletes a tombstone.
          this.#states.delete(oldestId);
          break;
        }
      }
    } else {
      this.#acknowledged.delete(id);
    }
    this.#marks += 1;
    if (this.#marks >= SWEEP_EVERY) {
      this.#marks = 0;
      this.#sweep(now);
    }
  }

  // Released and expired state is not bounded by count, because forgetting it could let a stale receipt reach SQS.
  // It is bounded by time instead: after SQS retention the message is gone, so the state can go with it.
  #sweep(now: number): void {
    for (const [id, state] of this.#states) {
      if (state.status === "leased") continue;
      if (now - state.settledAt < RETAINED_MS) continue;
      this.#states.delete(id);
      this.#acknowledged.delete(id);
    }
  }

  // A receipt is settled when it is not the latest one this instance handed out for its message id, when the message
  // was already settled, or when the lease expired. SQS accepts deletes and visibility changes with old handles, so
  // this check must run before either call.
  #isSettled(id: string, receipt: Receipt, now: number): boolean {
    const state = this.#states.get(id);
    if (state === undefined) return false;
    if (state.receipt !== receipt) return true;
    if (state.status !== "leased") return true;
    if (state.leaseUntil > now) return false;
    this.#mark(id, receipt, "expired");
    return true;
  }

  #queueUrl(name: string): Promise<string> {
    const cached = this.#urls.get(name);
    if (cached !== undefined) return cached;
    const digest = createHash("sha256").update(name).digest("hex").slice(0, 32);
    const queueName = `${this.#prefix}-${digest}`;
    const created = sqs()
      .then(({ CreateQueueCommand }) => this.#client.send(new CreateQueueCommand({ QueueName: queueName })))
      .then((reply) => {
        const url = reply.QueueUrl;
        if (!url) throw new Error(`SQS did not return a queue URL for ${queueName}`);
        return url;
      });
    created.catch(() => this.#urls.delete(name));
    this.#urls.set(name, created);
    return created;
  }

  #assertOpen(): void {
    if (this.#closed) throw new Error("SqsDialect is closed");
  }
}
