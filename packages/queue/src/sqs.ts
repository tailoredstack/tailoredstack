import { createHash, randomUUID } from "node:crypto";
import {
  ChangeMessageVisibilityCommand,
  CreateQueueCommand,
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  type Message,
  ReceiveMessageCommand,
  SendMessageCommand,
  type SQSClient,
} from "@aws-sdk/client-sqs";
import type { Driver, DriverCapabilities, DriverLease, QueueSize } from "./types.js";

// The part of an AWS SDK v3 SQS client the driver uses. The caller owns the client; close() destroys it.
export type SqsClient = Pick<SQSClient, "send" | "destroy">;

export interface SqsDriverOptions {
  readonly client: SqsClient;
  // Queue name `name` maps to SQS queue `${queueNamePrefix}-${32 hex digits}`. The prefix must keep the total under 80 characters.
  readonly queueNamePrefix: string;
}

export const SQS_CAPABILITIES: DriverCapabilities = {
  shared: true,
  exactCounts: false,
  maxPushDelayMs: 900_000,
};

// SQS has no ordering and no per-message lease, so every fact the contract needs lives in the message body.
// The payload is base64 so the body is valid SQS text whatever the payload holds.
interface StoredMessage {
  readonly id: string;
  readonly seq: number;
  readonly availableAt: number;
  readonly attempts: number;
  readonly payload: string;
}

// A lease this driver instance holds. It outlives its visibility so the attempt count carries over on the next reserve.
interface Lease {
  readonly name: string;
  readonly id: string;
  readonly seq: number;
  readonly attempts: number;
  readonly payload: string;
  readonly token: string;
  receipt: string;
  leaseUntil: number;
  // True once this instance has made the message visible again because the lease ended on the caller's clock.
  visible: boolean;
}

interface Candidate {
  readonly message: Message;
  readonly stored: StoredMessage;
}

// Drained messages stay hidden this long while the driver chooses one. Each one is restored before the call returns.
const HOLD_SECONDS = 30;
// SQS caps a visibility timeout at 12 hours.
const MAX_VISIBILITY_SECONDS = 43_200;
// Returned when the receipt is no longer the message's current receipt, for example after another instance received it again.
const STALE_RECEIPT_CODES: Record<string, true> = {
  ReceiptHandleIsInvalid: true,
  MessageNotInflight: true,
};

function isStaleReceipt(error: unknown): boolean {
  return error instanceof Error && STALE_RECEIPT_CODES[error.name] === true;
}

function visibilityFor(leaseUntil: number, now: number): number {
  return Math.min(MAX_VISIBILITY_SECONDS, Math.max(0, Math.ceil((leaseUntil - now) / 1000)));
}

function byDueThenSequence(a: Candidate, b: Candidate): number {
  return a.stored.availableAt - b.stored.availableAt || a.stored.seq - b.stored.seq;
}

export function createSqsDriver({ client, queueNamePrefix }: SqsDriverOptions): Driver {
  const urls = new Map<string, Promise<string>>();
  const chains = new Map<string, Promise<void>>();
  // Leases this instance holds, by message id, and the token each one was issued under.
  const leases = new Map<string, Lease>();
  const tokens = new Map<string, string>();
  let sequence = 0;

  function queueUrl(name: string): Promise<string> {
    let url = urls.get(name);
    if (url === undefined) {
      const digest = createHash("sha256").update(name).digest("hex").slice(0, 32);
      url = client
        .send(new CreateQueueCommand({ QueueName: `${queueNamePrefix}-${digest}` }))
        .then((reply) => reply.QueueUrl as string);
      urls.set(name, url);
      // A failed create is not cached, so the next call tries again.
      url.catch(() => urls.delete(name));
    }
    return url;
  }

  // Runs reserve and size calls for one queue one at a time. A drain hides messages, so overlapping calls would miss them.
  function serial<T>(name: string, task: () => Promise<T>): Promise<T> {
    const result = (chains.get(name) ?? Promise.resolve()).then(task);
    chains.set(
      name,
      result.then(
        () => undefined,
        () => undefined,
      ),
    );
    return result;
  }

  async function setVisibility(url: string, receipt: string, seconds: number): Promise<void> {
    await client.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl: url,
        ReceiptHandle: receipt,
        VisibilityTimeout: seconds,
      }),
    );
  }

  // Makes visible again the messages whose leases ended on the caller's clock. SQS only knows real time.
  async function releaseExpired(name: string, url: string, now: number): Promise<void> {
    for (const lease of leases.values()) {
      if (lease.name !== name || lease.visible || lease.leaseUntil > now) continue;
      try {
        await setVisibility(url, lease.receipt, 0);
      } catch (error) {
        if (!isStaleReceipt(error)) throw error;
      }
      lease.visible = true;
    }
  }

  // Receives every visible message of the queue. The first receive long-polls so an empty queue is seen before reserve
  // returns null; later receives return at once.
  async function drain(url: string): Promise<Message[]> {
    const messages: Message[] = [];
    for (let wait = 1; ; wait = 0) {
      const reply = await client.send(
        new ReceiveMessageCommand({
          QueueUrl: url,
          MaxNumberOfMessages: 10,
          VisibilityTimeout: HOLD_SECONDS,
          WaitTimeSeconds: wait,
        }),
      );
      const batch = reply.Messages ?? [];
      if (batch.length === 0) return messages;
      messages.push(...batch);
    }
  }

  // Sorts drained messages into due (available, not leased) and later. A message this instance leases live is hidden
  // again for the lease's remaining time, so SQS real-time expiry cannot free it early.
  async function classify(
    url: string,
    messages: Message[],
    now: number,
  ): Promise<{ due: Candidate[]; later: Candidate[] }> {
    const due: Candidate[] = [];
    const later: Candidate[] = [];
    for (const message of messages) {
      const stored = JSON.parse(message.Body as string) as StoredMessage;
      const held = leases.get(stored.id);
      if (held !== undefined && held.leaseUntil > now) {
        held.receipt = message.ReceiptHandle as string;
        held.visible = false;
        await setVisibility(url, held.receipt, visibilityFor(held.leaseUntil, now));
      } else if (stored.availableAt <= now) {
        due.push({ message, stored });
      } else {
        later.push({ message, stored });
      }
    }
    return { due, later };
  }

  // Restores drained messages this call does not return.
  async function restore(url: string, candidates: Candidate[]): Promise<void> {
    for (const { message } of candidates) {
      await setVisibility(url, message.ReceiptHandle as string, 0);
    }
  }

  function liveLease(name: string, token: string, now: number): Lease | undefined {
    const id = tokens.get(token);
    const lease = id === undefined ? undefined : leases.get(id);
    if (lease === undefined || lease.name !== name || lease.leaseUntil <= now) return undefined;
    return lease;
  }

  function forget(lease: Lease): void {
    leases.delete(lease.id);
    tokens.delete(lease.token);
  }

  return {
    capabilities: SQS_CAPABILITIES,

    async push(name, payload, { availableAt }) {
      const url = await queueUrl(name);
      const id = randomUUID();
      sequence += 1;
      const body: StoredMessage = {
        id,
        seq: sequence,
        availableAt,
        attempts: 0,
        payload: Buffer.from(payload, "utf8").toString("base64"),
      };
      await client.send(new SendMessageCommand({ QueueUrl: url, MessageBody: JSON.stringify(body) }));
      return id;
    },

    reserve(name, now, leaseUntil) {
      return serial(name, async (): Promise<DriverLease | null> => {
        const url = await queueUrl(name);
        await releaseExpired(name, url, now);
        const { due, later } = await classify(url, await drain(url), now);
        due.sort(byDueThenSequence);
        const winner = due.shift();
        await restore(url, [...due, ...later]);
        if (winner === undefined) return null;
        const { message, stored } = winner;
        const receipt = message.ReceiptHandle as string;
        const prior = leases.get(stored.id);
        const attempts = (prior?.attempts ?? stored.attempts) + 1;
        const token = randomUUID();
        if (prior !== undefined) tokens.delete(prior.token);
        const lease: Lease = {
          name,
          id: stored.id,
          seq: stored.seq,
          attempts,
          payload: stored.payload,
          token,
          receipt,
          leaseUntil,
          visible: false,
        };
        leases.set(stored.id, lease);
        tokens.set(token, stored.id);
        await setVisibility(url, receipt, visibilityFor(leaseUntil, now));
        return {
          id: stored.id,
          payload: Buffer.from(stored.payload, "base64").toString("utf8"),
          attempts,
          token,
        };
      });
    },

    async delete(name, token, now) {
      const lease = liveLease(name, token, now);
      if (lease === undefined) return false;
      forget(lease);
      await client.send(new DeleteMessageCommand({ QueueUrl: await queueUrl(name), ReceiptHandle: lease.receipt }));
      return true;
    },

    async release(name, token, availableAt, now) {
      const lease = liveLease(name, token, now);
      if (lease === undefined) return false;
      forget(lease);
      const url = await queueUrl(name);
      const body: StoredMessage = {
        id: lease.id,
        seq: lease.seq,
        availableAt,
        attempts: lease.attempts,
        payload: lease.payload,
      };
      // Not atomic: if DeleteMessage fails after SendMessage succeeds, the message can be delivered twice.
      await client.send(new SendMessageCommand({ QueueUrl: url, MessageBody: JSON.stringify(body) }));
      await client.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: lease.receipt }));
      return true;
    },

    size(name, now) {
      return serial(name, async (): Promise<QueueSize> => {
        const url = await queueUrl(name);
        await releaseExpired(name, url, now);
        // Read before draining: the drain hides messages, which would otherwise count as reserved.
        const attributes = await client.send(
          new GetQueueAttributesCommand({
            QueueUrl: url,
            AttributeNames: ["ApproximateNumberOfMessagesNotVisible"],
          }),
        );
        const reserved = Number(attributes.Attributes?.ApproximateNumberOfMessagesNotVisible);
        const { due, later } = await classify(url, await drain(url), now);
        await restore(url, [...due, ...later]);
        return { ready: due.length, delayed: later.length, reserved };
      });
    },

    async close() {
      client.destroy();
    },
  };
}
