import { createHash, randomUUID } from "node:crypto";
import {
  ChangeMessageVisibilityCommand,
  CreateQueueCommand,
  DeleteMessageCommand,
  type Message,
  ReceiveMessageCommand,
  SendMessageCommand,
  type SQSClient,
} from "@aws-sdk/client-sqs";
import type { Driver, DriverCapabilities, QueueSize } from "../types.js";

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

// Every fact the driver needs lives in the message body. An SQS body cannot change, so each change sends a new copy and
// deletes the old one. `version` tells copies of one message apart; the highest version is the current copy.
// The payload is base64 so the body is valid SQS text whatever the payload holds.
interface StoredMessage {
  readonly id: string;
  readonly payload: string;
  readonly pushSeq: number;
  readonly version: number;
  readonly availableAt: number;
  readonly attempts: number;
  readonly leaseUntil: number;
  readonly token: string | null;
}

interface Copy {
  readonly message: Message;
  readonly stored: StoredMessage;
}

// Drained messages stay hidden this long while one is chosen. Every drained copy is restored, deleted, or replaced before
// the call returns, so the hold only matters when a call runs longer than this.
const HOLD_SECONDS = 30;
// Returned when the receipt no longer belongs to the message's current receipt, for example after another instance
// received the message again. The newer copy is then authoritative, so the stale receipt is ignored.
const STALE_RECEIPT_CODES: Record<string, true> = {
  ReceiptHandleIsInvalid: true,
  MessageNotInflight: true,
};

function isStaleReceipt(error: unknown): boolean {
  return error instanceof Error && STALE_RECEIPT_CODES[error.name] === true;
}

// A lease is live on the caller's clock. SQS visibility is not part of the lease, so every instance decides the same way.
function isLive(stored: StoredMessage, now: number): boolean {
  return stored.token !== null && stored.leaseUntil > now;
}

function isDue(stored: StoredMessage, now: number): boolean {
  return stored.availableAt <= now && !isLive(stored, now);
}

function byDueThenSequence(a: Copy, b: Copy): number {
  return a.stored.availableAt - b.stored.availableAt || a.stored.pushSeq - b.stored.pushSeq;
}

function parseCopy(message: Message): Copy {
  return { message, stored: JSON.parse(message.Body as string) as StoredMessage };
}

// Keeps the highest-version copy of each message. A lower copy is left behind when a rewrite sent its replacement and
// then failed to delete it; it is removed and never chosen.
function splitCopies(copies: Copy[]): { current: Copy[]; superseded: Copy[] } {
  const newest = new Map<string, Copy>();
  const superseded: Copy[] = [];
  for (const copy of copies) {
    const held = newest.get(copy.stored.id);
    if (held === undefined) {
      newest.set(copy.stored.id, copy);
    } else if (copy.stored.version > held.stored.version) {
      superseded.push(held);
      newest.set(copy.stored.id, copy);
    } else {
      superseded.push(copy);
    }
  }
  return { current: [...newest.values()], superseded };
}

export function createSqsDriver({ client, queueNamePrefix }: SqsDriverOptions): Driver {
  const urls = new Map<string, Promise<string>>();
  const chains = new Map<string, Promise<void>>();
  // Per instance. SQS has no counter shared by instances, so pushes from two instances can share a sequence number.
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

  // Runs reserve, delete, release, and size calls for one queue one at a time. A drain hides messages, so overlapping
  // calls would miss them.
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

  // Receives every visible message of the queue. Each receive long-polls, so an empty receive means nothing is visible
  // now; the drained messages stay hidden, so the drain ends.
  async function drain(url: string): Promise<Message[]> {
    const messages: Message[] = [];
    for (;;) {
      const reply = await client.send(
        new ReceiveMessageCommand({
          QueueUrl: url,
          MaxNumberOfMessages: 10,
          VisibilityTimeout: HOLD_SECONDS,
          WaitTimeSeconds: 1,
        }),
      );
      const batch = reply.Messages ?? [];
      if (batch.length === 0) return messages;
      messages.push(...batch);
    }
  }

  // Drains the queue, removes superseded copies, and returns the current copy of every message. Every returned copy is
  // hidden until the caller restores, deletes, or replaces it.
  async function scan(url: string): Promise<Copy[]> {
    const { current, superseded } = splitCopies((await drain(url)).map(parseCopy));
    for (const copy of superseded) {
      await remove(url, copy.message);
    }
    return current;
  }

  async function remove(url: string, message: Message): Promise<void> {
    try {
      await client.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: message.ReceiptHandle }));
    } catch (error) {
      if (!isStaleReceipt(error)) throw error;
    }
  }

  async function restore(url: string, message: Message): Promise<void> {
    try {
      await client.send(
        new ChangeMessageVisibilityCommand({
          QueueUrl: url,
          ReceiptHandle: message.ReceiptHandle,
          VisibilityTimeout: 0,
        }),
      );
    } catch (error) {
      if (!isStaleReceipt(error)) throw error;
    }
  }

  // SQS cannot change a body. The replacement is sent first, so a failure leaves the old copy rather than nothing.
  async function rewrite(url: string, copy: Copy, next: StoredMessage): Promise<void> {
    await client.send(new SendMessageCommand({ QueueUrl: url, MessageBody: JSON.stringify(next) }));
    await remove(url, copy.message);
  }

  // Finds the live copy whose token matches. Other copies are made visible again, so nothing stays hidden.
  async function holder(url: string, copies: Copy[], token: string, now: number): Promise<Copy | undefined> {
    const target = copies.find((copy) => copy.stored.token === token && isLive(copy.stored, now));
    for (const copy of copies) {
      if (copy !== target) await restore(url, copy.message);
    }
    return target;
  }

  return {
    capabilities: SQS_CAPABILITIES,

    push(name, payload, { availableAt, now }) {
      return queueUrl(name).then(async (url) => {
        sequence += 1;
        const stored: StoredMessage = {
          id: randomUUID(),
          payload: Buffer.from(payload, "utf8").toString("base64"),
          pushSeq: sequence,
          version: 0,
          availableAt: Math.max(availableAt, now),
          attempts: 0,
          leaseUntil: 0,
          token: null,
        };
        await client.send(new SendMessageCommand({ QueueUrl: url, MessageBody: JSON.stringify(stored) }));
        return stored.id;
      });
    },

    reserve(name, now, leaseUntil) {
      return serial(name, async () => {
        const url = await queueUrl(name);
        const copies = await scan(url);
        const [winner] = copies.filter((copy) => isDue(copy.stored, now)).sort(byDueThenSequence);
        for (const copy of copies) {
          if (copy !== winner) await restore(url, copy.message);
        }
        if (winner === undefined) return null;
        const token = randomUUID();
        const attempts = winner.stored.attempts + 1;
        const next: StoredMessage = {
          ...winner.stored,
          attempts,
          leaseUntil,
          token,
          version: winner.stored.version + 1,
        };
        await rewrite(url, winner, next);
        return {
          id: next.id,
          payload: Buffer.from(next.payload, "base64").toString("utf8"),
          attempts,
          token,
        };
      });
    },

    delete(name, token, now) {
      return serial(name, async () => {
        const url = await queueUrl(name);
        const copies = await scan(url);
        const target = await holder(url, copies, token, now);
        if (target === undefined) return false;
        await remove(url, target.message);
        return true;
      });
    },

    release(name, token, availableAt, now) {
      return serial(name, async () => {
        const url = await queueUrl(name);
        const copies = await scan(url);
        const target = await holder(url, copies, token, now);
        if (target === undefined) return false;
        const next: StoredMessage = {
          ...target.stored,
          availableAt: Math.max(availableAt, now),
          leaseUntil: 0,
          token: null,
          version: target.stored.version + 1,
        };
        await rewrite(url, target, next);
        return true;
      });
    },

    size(name, now) {
      return serial(name, async () => {
        const url = await queueUrl(name);
        const copies = await scan(url);
        const size: QueueSize = { ready: 0, delayed: 0, reserved: 0 };
        for (const { stored } of copies) {
          if (isLive(stored, now)) size.reserved += 1;
          else if (stored.availableAt <= now) size.ready += 1;
          else size.delayed += 1;
        }
        for (const copy of copies) {
          await restore(url, copy.message);
        }
        return size;
      });
    },

    async close() {
      client.destroy();
    },
  };
}
