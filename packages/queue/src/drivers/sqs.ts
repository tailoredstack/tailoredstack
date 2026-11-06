import type { QueueCounts, QueueDriver, RawFailedJob, RawJob } from "../driver.ts";

const MAX_DELAY = 900;
const MAX_VISIBILITY = 43_200;

export interface SendInput {
  readonly QueueUrl: string;
  readonly MessageBody: string;
  readonly DelaySeconds?: number;
}
export interface ReceiveInput {
  readonly QueueUrl: string;
  readonly MaxNumberOfMessages?: number;
  readonly AttributeNames?: readonly string[];
  readonly VisibilityTimeout?: number;
}
export interface DeleteInput {
  readonly QueueUrl: string;
  readonly ReceiptHandle: string;
}
export interface ChangeVisibilityInput extends DeleteInput {
  readonly VisibilityTimeout: number;
}
export interface GetAttributesInput {
  readonly QueueUrl: string;
  readonly AttributeNames: readonly string[];
}

/** Minimal injected structural SQS client; no AWS SDK runtime dependency. */
export interface SqsClient {
  sendMessage(input: SendInput): Promise<{ MessageId?: string }>;
  receiveMessage(input: ReceiveInput): Promise<{ Messages?: Record<string, unknown>[] }>;
  deleteMessage(input: DeleteInput): Promise<unknown>;
  changeMessageVisibility(input: ChangeVisibilityInput): Promise<unknown>;
  getQueueAttributes(input: GetAttributesInput): Promise<{ Attributes?: Record<string, string> }>;
}
export interface SqsQueueDriverOptions {
  readonly client: SqsClient;
  readonly queueUrl: (queue: string) => string;
  /** Separate standard SQS queue used as the failed store. */
  readonly failedQueueUrl: string;
}
interface Envelope {
  readonly kind: "job" | "failed";
  readonly id?: string;
  readonly queue: string;
  readonly payload: string;
  readonly attempts?: number;
  readonly error?: string;
  readonly failedAt?: number;
}
interface Lease {
  readonly queue: string;
  readonly messageId: string;
  readonly payload: string;
  readonly attempts: number;
  readonly reservedUntil: number;
}

/** Standard SQS driver. Reserved ids are ReceiptHandles scoped to a reservation. */
export class SqsQueueDriver implements QueueDriver {
  readonly #client: SqsClient;
  readonly #queueUrl: (queue: string) => string;
  readonly #failedQueueUrl: string;
  // Locally cached lease metadata; SQS receipt handles remain the externally returned reservation ids.
  readonly #leases = new Map<string, Lease>();
  readonly #failedCache = new Map<string, RawFailedJob>();
  readonly #failedReceipts = new Map<string, string>();

  constructor({ client, queueUrl, failedQueueUrl }: SqsQueueDriverOptions) {
    this.#client = client;
    this.#queueUrl = queueUrl;
    this.#failedQueueUrl = failedQueueUrl;
  }

  async insert(queue: string, payload: string, availableAt: number, now?: number): Promise<string> {
    if (now === undefined && availableAt > 0)
      throw new Error("SQS insert requires now as the fourth argument to compute delay");
    const delayMs = now === undefined ? 0 : availableAt - now;
    if (delayMs > MAX_DELAY * 1_000) throw new RangeError("SQS delay cannot exceed 900 seconds");
    if (delayMs < 0) throw new RangeError("availableAt must not precede now");
    const response = await this.#client.sendMessage({
      QueueUrl: this.#queueUrl(queue),
      MessageBody: JSON.stringify({ kind: "job", queue, payload }),
      ...(delayMs > 0 ? { DelaySeconds: Math.ceil(delayMs / 1_000) } : {}),
    });
    if (!response.MessageId) throw new Error("SQS SendMessage returned no MessageId");
    return response.MessageId;
  }

  async reserve(queue: string, now: number, reservedUntil: number): Promise<RawJob | null> {
    const visibility = Math.ceil((reservedUntil - now) / 1_000);
    if (visibility > MAX_VISIBILITY) throw new RangeError("SQS visibility timeout cannot exceed 12h (43200 seconds)");
    if (visibility <= 0) throw new RangeError("SQS lease must have a positive visibility timeout");
    const response = await this.#client.receiveMessage({
      QueueUrl: this.#queueUrl(queue),
      MaxNumberOfMessages: 1,
      AttributeNames: ["ApproximateReceiveCount"],
      VisibilityTimeout: visibility,
    });
    const message = response.Messages?.[0];
    if (!message) return null;
    const id = requiredString(message.ReceiptHandle, "SQS ReceiptHandle");
    const messageId = requiredString(message.MessageId, "SQS MessageId");
    const envelope = parseEnvelope(requiredString(message.Body, "SQS message body"));
    if (envelope.kind !== "job" || envelope.queue !== queue) throw new Error("unexpected SQS queue message");
    const attempts = positiveInt((message.Attributes as Record<string, unknown> | undefined)?.ApproximateReceiveCount);
    this.#leases.set(id, { queue, messageId, payload: envelope.payload, attempts, reservedUntil });
    return { id, queue, payload: envelope.payload, attempts };
  }

  async remove(queue: string, id: string, attempts: number, now: number): Promise<boolean> {
    const lease = this.#held(queue, id, attempts, now);
    if (!lease) return false;
    await this.#client.deleteMessage({ QueueUrl: this.#queueUrl(queue), ReceiptHandle: id });
    this.#leases.delete(id);
    return true;
  }

  async release(queue: string, id: string, attempts: number, now: number, availableAt: number): Promise<boolean> {
    const lease = this.#held(queue, id, attempts, now);
    if (!lease) return false;
    const delayMs = availableAt - now;
    if (delayMs > MAX_DELAY * 1_000) throw new RangeError("SQS delay cannot exceed 900 seconds");
    if (delayMs < 0) throw new RangeError("availableAt must not precede now");
    await this.#client.sendMessage({
      QueueUrl: this.#queueUrl(queue),
      MessageBody: JSON.stringify({ kind: "job", queue, payload: lease.payload }),
      ...(delayMs > 0 ? { DelaySeconds: Math.ceil(delayMs / 1_000) } : {}),
    });
    await this.#client.deleteMessage({ QueueUrl: this.#queueUrl(queue), ReceiptHandle: id });
    this.#leases.delete(id);
    return true;
  }

  async fail(queue: string, id: string, attempts: number, now: number, error: string): Promise<boolean> {
    const lease = this.#held(queue, id, attempts, now);
    if (!lease) return false;
    const body = JSON.stringify({
      kind: "failed",
      id: lease.messageId,
      queue,
      payload: lease.payload,
      attempts,
      error,
      failedAt: now,
    });
    const response = await this.#client.sendMessage({ QueueUrl: this.#failedQueueUrl, MessageBody: body });
    if (!response.MessageId) throw new Error("SQS SendMessage returned no failed MessageId");
    this.#failedCache.set(lease.messageId, {
      id: lease.messageId,
      queue,
      payload: lease.payload,
      attempts,
      error,
      failedAt: now,
    });
    this.#failedReceipts.set(lease.messageId, response.MessageId);
    await this.#client.deleteMessage({ QueueUrl: this.#queueUrl(queue), ReceiptHandle: id });
    this.#leases.delete(id);
    return true;
  }

  async failedJobs(queue: string | null): Promise<RawFailedJob[]> {
    return [...this.#failedCache.values()].filter((job) => queue === null || job.queue === queue);
  }

  async retryFailed(id: string, now: number): Promise<boolean> {
    const failed = this.#failedCache.get(id);
    if (!failed) return false;
    await this.insert(failed.queue, failed.payload, now, now);
    await this.#deleteFailed(id);
    return true;
  }

  async retryAllFailed(queue: string | null, now: number): Promise<number> {
    const jobs = [...this.#failedCache.values()].filter((job) => queue === null || job.queue === queue);
    for (const job of jobs) await this.retryFailed(job.id, now);
    return jobs.length;
  }

  async forgetFailed(id: string): Promise<boolean> {
    return this.#deleteFailed(id);
  }

  async flushFailed(queue: string | null): Promise<number> {
    const ids = [...this.#failedCache.values()]
      .filter((job) => queue === null || job.queue === queue)
      .map((job) => job.id);
    for (const id of ids) await this.#deleteFailed(id);
    return ids.length;
  }

  async pruneFailed(cutoff: number): Promise<number> {
    const ids = [...this.#failedCache.values()].filter((job) => job.failedAt < cutoff).map((job) => job.id);
    for (const id of ids) await this.#deleteFailed(id);
    return ids.length;
  }

  async counts(queue: string, _now: number): Promise<QueueCounts> {
    const response = await this.#client.getQueueAttributes({
      QueueUrl: this.#queueUrl(queue),
      AttributeNames: [
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesDelayed",
        "ApproximateNumberOfMessagesNotVisible",
      ],
    });
    const attrs = response.Attributes ?? {};
    return {
      pending: count(attrs.ApproximateNumberOfMessages),
      delayed: count(attrs.ApproximateNumberOfMessagesDelayed),
      reserved: count(attrs.ApproximateNumberOfMessagesNotVisible),
    };
  }

  #held(queue: string, id: string, attempts: number, now: number): Lease | null {
    const lease = this.#leases.get(id);
    return lease && lease.queue === queue && lease.attempts === attempts && lease.reservedUntil > now ? lease : null;
  }

  async #deleteFailed(id: string): Promise<boolean> {
    const receipt = this.#failedReceipts.get(id);
    if (!receipt || !this.#failedCache.has(id)) return false;
    await this.#client.deleteMessage({ QueueUrl: this.#failedQueueUrl, ReceiptHandle: receipt });
    this.#failedCache.delete(id);
    this.#failedReceipts.delete(id);
    return true;
  }
}

function parseEnvelope(body: string): Envelope {
  const value = JSON.parse(body) as Record<string, unknown>;
  if (
    (value.kind === "job" || value.kind === "failed") &&
    typeof value.queue === "string" &&
    typeof value.payload === "string"
  )
    return value as unknown as Envelope;
  throw new Error("invalid SQS queue message envelope");
}
function requiredString(value: unknown, label: string): string {
  if (typeof value !== "string" || !value) throw new Error(`${label} is missing`);
  return value;
}
function positiveInt(value: unknown): number {
  const n = Number(value);
  return Number.isInteger(n) && n > 0 ? n : 1;
}
function count(value: string | undefined): number {
  const n = Number(value ?? 0);
  return Number.isFinite(n) && n >= 0 ? n : 0;
}
