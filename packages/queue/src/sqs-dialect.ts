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

let sdk: Promise<SqsSdk> | undefined;

function sqs(): Promise<SqsSdk> {
  if (sdk === undefined) sdk = import("@aws-sdk/client-sqs");
  return sdk;
}

const SECOND_MS = 1_000;
const MAX_DELAY_SECONDS = 900;
const MAX_LEASE_SECONDS = 43_200;
const SETTLED_LIMIT = 1_024;
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

interface Lease {
  readonly id: string;
  readonly leaseUntil: number;
}

export class SqsDialect implements Dialect {
  readonly capabilities: DialectCapabilities = {
    ordered: false,
    exactCounts: false,
    clockSource: "backend",
    maxDelayMs: MAX_DELAY_SECONDS * SECOND_MS,
    maxLeaseMs: MAX_LEASE_SECONDS * SECOND_MS,
  };

  readonly #client: SqsClient;
  readonly #prefix: string;
  readonly #urls = new Map<string, Promise<string>>();
  readonly #leases = new Map<Receipt, Lease>();
  readonly #current = new Map<string, Receipt>();
  readonly #settled = new Set<Receipt>();
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
    const receipt = `${id}:${handle}` as Receipt;
    const previous = this.#current.get(id);
    if (previous !== undefined) this.#settle(id, previous);
    // The backend owns the lease: record the visibility SQS actually granted, not the caller's timestamp.
    this.#leases.set(receipt, { id, leaseUntil: now + leaseSeconds * SECOND_MS });
    this.#current.set(id, receipt);
    return {
      id,
      payload: message.Body ?? "",
      attempts: Number(message.Attributes?.ApproximateReceiveCount ?? "1"),
      receipt,
    };
  }

  async acknowledge(name: string, receipt: Receipt, _now: number): Promise<boolean> {
    this.#assertOpen();
    const split = splitReceipt(receipt);
    if (split === null) return false;
    if (this.#isSettled(receipt, Date.now())) return false;
    const { DeleteMessageCommand } = await sqs();
    const url = await this.#queueUrl(name);
    try {
      await this.#client.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: split.handle }));
    } catch (error) {
      if (!isStaleReceipt(error)) throw error;
      this.#settle(split.id, receipt);
      return false;
    }
    this.#settle(split.id, receipt);
    return true;
  }

  async release(name: string, receipt: Receipt, at: { availableAt: number; now: number }): Promise<boolean> {
    this.#assertOpen();
    const split = splitReceipt(receipt);
    if (split === null) return false;
    if (this.#isSettled(receipt, Date.now())) return false;
    const { ChangeMessageVisibilityCommand } = await sqs();
    const url = await this.#queueUrl(name);
    try {
      await this.#client.send(
        new ChangeMessageVisibilityCommand({
          QueueUrl: url,
          ReceiptHandle: split.handle,
          VisibilityTimeout: secondsUntil(at.availableAt, Date.now(), MAX_LEASE_SECONDS),
        }),
      );
    } catch (error) {
      if (!isStaleReceipt(error)) throw error;
      this.#settle(split.id, receipt);
      return false;
    }
    this.#settle(split.id, receipt);
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
    return {
      ready: parseCount(attributes.ApproximateNumberOfMessages),
      delayed: parseCount(attributes.ApproximateNumberOfMessagesDelayed),
      reserved: parseCount(attributes.ApproximateNumberOfMessagesNotVisible),
    };
  }

  async close(): Promise<void> {
    this.#closed = true;
  }

  // A lease this instance handed out stays authoritative until it expires: SQS accepts a delete with an old handle, so
  // without this check a stale receipt could remove a message another consumer now holds.
  #isSettled(receipt: Receipt, now: number): boolean {
    const lease = this.#leases.get(receipt);
    if (lease === undefined) return this.#settled.has(receipt);
    if (lease.leaseUntil > now) return false;
    this.#settle(lease.id, receipt);
    return true;
  }

  #settle(id: string, receipt: Receipt): void {
    this.#leases.delete(receipt);
    if (this.#current.get(id) === receipt) this.#current.delete(id);
    this.#settled.add(receipt);
    if (this.#settled.size > SETTLED_LIMIT) {
      const [oldest] = this.#settled;
      this.#settled.delete(oldest);
    }
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
