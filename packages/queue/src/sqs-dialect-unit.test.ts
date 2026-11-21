import {
  ChangeMessageVisibilityCommand,
  CreateQueueCommand,
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
} from "@aws-sdk/client-sqs";
import { describe, expect, it, vi } from "vitest";
import type { Receipt } from "./dialect.ts";
import { type SqsClient, SqsDialect } from "./sqs-dialect.ts";

type Reply = (command: unknown) => unknown;

function fakeClient(...replies: Reply[]): { client: SqsClient; calls: unknown[] } {
  const calls: unknown[] = [];
  const client = {
    async send(command: unknown): Promise<unknown> {
      calls.push(command);
      const reply = replies.shift();
      if (reply === undefined) throw new Error("unexpected SQS call");
      return reply(command);
    },
  } as unknown as SqsClient;
  return { client, calls };
}

const queueUrl = (): { QueueUrl: string } => ({ QueueUrl: "https://sqs.test/queue" });
const receipt = (value: string): Receipt => value as Receipt;
const now = Date.now();

function sqsError(name: string): Error {
  const error = new Error(name);
  error.name = name;
  return error;
}

function input<C extends { readonly input: unknown }>(
  call: unknown,
  type: abstract new (...args: never[]) => C,
): C["input"] {
  expect(call).toBeInstanceOf(type);
  return (call as C).input;
}

describe("SqsDialect unit", () => {
  it("rejects an unusable queue name prefix and accepts the default", () => {
    const { client } = fakeClient();
    for (const prefix of ["", "a".repeat(48), "bad prefix", "bad.prefix"]) {
      expect(() => new SqsDialect({ client, queueNamePrefix: prefix })).toThrow(TypeError);
    }
    expect(() => new SqsDialect({ client, queueNamePrefix: "a".repeat(47) })).not.toThrow();
    expect(() => new SqsDialect({ client })).not.toThrow();
  });

  it("creates one queue per name, hashes the name, and clamps the delay", async () => {
    const { client, calls } = fakeClient(
      queueUrl,
      () => ({ MessageId: "one" }),
      () => ({ MessageId: "two" }),
    );
    const d = new SqsDialect({ client, queueNamePrefix: "app" });

    expect(await d.enqueue("jobs", "body", { availableAt: now - 1_000, now })).toBe("one");
    expect(await d.enqueue("jobs", "body", { availableAt: now + 3_600_000, now })).toBe("two");

    expect(input(calls[0], CreateQueueCommand).QueueName).toMatch(/^app-[0-9a-f]{32}$/);
    expect(input(calls[1], SendMessageCommand).DelaySeconds).toBe(0);
    expect(input(calls[2], SendMessageCommand).DelaySeconds).toBe(900);
    expect(calls).toHaveLength(3);
  });

  it("does not cache a failed queue creation and rejects a missing queue URL", async () => {
    const { client, calls } = fakeClient(
      () => {
        throw new Error("create failed");
      },
      queueUrl,
      () => ({ MessageId: "one" }),
    );
    const d = new SqsDialect({ client });
    await expect(d.enqueue("jobs", "body", { availableAt: now, now })).rejects.toThrow("create failed");
    expect(await d.enqueue("jobs", "body", { availableAt: now, now })).toBe("one");
    expect(calls[0]).toBeInstanceOf(CreateQueueCommand);
    expect(calls[1]).toBeInstanceOf(CreateQueueCommand);

    const missing = fakeClient(() => ({}));
    await expect(
      new SqsDialect({ client: missing.client }).enqueue("jobs", "body", { availableAt: now, now }),
    ).rejects.toThrow(/queue URL/);
  });

  it("rejects enqueue when SQS returns no message id", async () => {
    const { client } = fakeClient(queueUrl, () => ({}));
    await expect(new SqsDialect({ client }).enqueue("jobs", "body", { availableAt: now, now })).rejects.toThrow(
      /message id/,
    );
  });

  it("returns null when a receive has no messages", async () => {
    for (const reply of [() => ({ Messages: [] }), () => ({})]) {
      const { client } = fakeClient(queueUrl, reply);
      await expect(new SqsDialect({ client }).dequeue("jobs", { now, leaseUntil: now + 1_000 })).resolves.toBeNull();
    }
  });

  it("reads attempts and body, defaults a missing receive count, and clamps the lease", async () => {
    const { client, calls } = fakeClient(
      queueUrl,
      () => ({ Messages: [{ MessageId: "one", ReceiptHandle: "handle", Body: "body" }] }),
      () => ({
        Messages: [
          {
            MessageId: "two",
            ReceiptHandle: "handle-2",
            Body: "body-2",
            Attributes: { ApproximateReceiveCount: "3" },
          },
        ],
      }),
      () => ({ Messages: [{ MessageId: "three", ReceiptHandle: "handle-3", Body: "body" }] }),
    );
    const d = new SqsDialect({ client });

    await expect(d.dequeue("jobs", { now, leaseUntil: now + 999_999_999 })).resolves.toEqual({
      id: "one",
      payload: "body",
      attempts: 1,
      receipt: receipt("one:handle"),
    });
    expect(input(calls[1], ReceiveMessageCommand).VisibilityTimeout).toBe(43_200);

    await expect(d.dequeue("jobs", { now, leaseUntil: now + 1_000 })).resolves.toEqual({
      id: "two",
      payload: "body-2",
      attempts: 3,
      receipt: receipt("two:handle-2"),
    });
    expect(input(calls[2], ReceiveMessageCommand).VisibilityTimeout).toBe(1);

    await d.dequeue("jobs", { now, leaseUntil: now - 1_000 });
    expect(input(calls[3], ReceiveMessageCommand).VisibilityTimeout).toBe(0);
  });

  it("rejects a received message without an id or a receipt handle", async () => {
    for (const message of [
      { ReceiptHandle: "handle", Body: "body" },
      { MessageId: "one", Body: "body" },
    ]) {
      const { client } = fakeClient(queueUrl, () => ({ Messages: [message] }));
      await expect(new SqsDialect({ client }).dequeue("jobs", { now, leaseUntil: now + 1_000 })).rejects.toThrow(
        /without an id or receipt handle/,
      );
    }
  });

  it("rejects a received message without a body", async () => {
    const { client } = fakeClient(queueUrl, () => ({
      Messages: [{ MessageId: "one", ReceiptHandle: "handle" }],
    }));
    await expect(new SqsDialect({ client }).dequeue("jobs", { now, leaseUntil: now + 1_000 })).rejects.toThrow(
      /without a body/,
    );
  });

  it("rejects malformed receipts without touching SQS", async () => {
    const { client, calls } = fakeClient();
    const d = new SqsDialect({ client });
    for (const bad of ["no-separator", ":handle", "id:"]) {
      await expect(d.acknowledge("jobs", receipt(bad), now)).resolves.toBe(false);
      await expect(d.release("jobs", receipt(bad), { availableAt: now, now })).resolves.toBe(false);
    }
    expect(calls).toHaveLength(0);
  });

  it("acknowledges with the SQS handle and treats the same receipt as settled", async () => {
    const { client, calls } = fakeClient(
      queueUrl,
      () => ({ Messages: [{ MessageId: "one", ReceiptHandle: "handle", Body: "body" }] }),
      () => ({}),
    );
    const d = new SqsDialect({ client });
    const message = await d.dequeue("jobs", { now, leaseUntil: now + 1_000 });
    if (!message) throw new Error("expected a message");

    await expect(d.acknowledge("jobs", message.receipt, now + 500)).resolves.toBe(true);
    expect(input(calls[2], DeleteMessageCommand).ReceiptHandle).toBe("handle");
    await expect(d.acknowledge("jobs", message.receipt, now + 500)).resolves.toBe(false);
    expect(calls).toHaveLength(3);
  });

  it("acknowledges an unknown but well-formed receipt through SQS", async () => {
    const { client, calls } = fakeClient(queueUrl, () => ({}));
    await expect(new SqsDialect({ client }).acknowledge("jobs", receipt("remote:handle"), now)).resolves.toBe(true);
    expect(input(calls[1], DeleteMessageCommand).ReceiptHandle).toBe("handle");
  });

  it("returns false for a stale receipt and rethrows other delete errors", async () => {
    for (const name of ["ReceiptHandleIsInvalid", "MessageNotInflight"]) {
      const { client } = fakeClient(queueUrl, () => {
        throw sqsError(name);
      });
      await expect(new SqsDialect({ client }).acknowledge("jobs", receipt("remote:handle"), now)).resolves.toBe(false);
    }

    const { client } = fakeClient(queueUrl, () => {
      throw sqsError("AccessDenied");
    });
    await expect(new SqsDialect({ client }).acknowledge("jobs", receipt("remote:handle"), now)).rejects.toThrow(
      "AccessDenied",
    );
  });

  it("returns false for an acknowledge after lease expiry without deleting", async () => {
    const { client, calls } = fakeClient(queueUrl, () => ({
      Messages: [{ MessageId: "one", ReceiptHandle: "handle", Body: "body" }],
    }));
    const d = new SqsDialect({ client });
    const message = await d.dequeue("jobs", { now, leaseUntil: now - 1_000 });
    if (!message) throw new Error("expected a message");

    await expect(d.acknowledge("jobs", message.receipt, now)).resolves.toBe(false);
    expect(calls).toHaveLength(2);
  });

  it("releases with a visibility delay, uses the handle, and treats the receipt as settled", async () => {
    const { client, calls } = fakeClient(
      queueUrl,
      () => ({ Messages: [{ MessageId: "one", ReceiptHandle: "handle", Body: "body" }] }),
      () => ({}),
    );
    const d = new SqsDialect({ client });
    const message = await d.dequeue("jobs", { now, leaseUntil: now + 1_000 });
    if (!message) throw new Error("expected a message");

    await expect(d.release("jobs", message.receipt, { availableAt: now + 5_000, now: now + 500 })).resolves.toBe(true);
    expect(input(calls[2], ChangeMessageVisibilityCommand)).toMatchObject({
      ReceiptHandle: "handle",
      VisibilityTimeout: 5,
    });
    await expect(d.release("jobs", message.receipt, { availableAt: now + 5_000, now: now + 500 })).resolves.toBe(false);
    expect(calls).toHaveLength(3);
  });

  it("returns false for a stale release, an expired lease, and rethrows other errors", async () => {
    for (const name of ["ReceiptHandleIsInvalid", "MessageNotInflight"]) {
      const { client } = fakeClient(queueUrl, () => {
        throw sqsError(name);
      });
      await expect(
        new SqsDialect({ client }).release("jobs", receipt("remote:handle"), { availableAt: now, now }),
      ).resolves.toBe(false);
    }

    const failing = fakeClient(queueUrl, () => {
      throw sqsError("AccessDenied");
    });
    await expect(
      new SqsDialect({ client: failing.client }).release("jobs", receipt("remote:handle"), {
        availableAt: now,
        now,
      }),
    ).rejects.toThrow("AccessDenied");

    const { client, calls } = fakeClient(queueUrl, () => ({
      Messages: [{ MessageId: "one", ReceiptHandle: "handle", Body: "body" }],
    }));
    const d = new SqsDialect({ client });
    const message = await d.dequeue("jobs", { now, leaseUntil: now - 1_000 });
    if (!message) throw new Error("expected a message");
    await expect(d.release("jobs", message.receipt, { availableAt: now + 2_000, now })).resolves.toBe(false);
    expect(calls).toHaveLength(2);
  });

  it("settles the earlier receipt when the same message is delivered again", async () => {
    const { client } = fakeClient(
      queueUrl,
      () => ({ Messages: [{ MessageId: "one", ReceiptHandle: "first", Body: "body" }] }),
      () => ({ Messages: [{ MessageId: "one", ReceiptHandle: "second", Body: "body" }] }),
      () => ({}),
    );
    const d = new SqsDialect({ client });
    const first = await d.dequeue("jobs", { now, leaseUntil: now + 1_000 });
    const second = await d.dequeue("jobs", { now: now + 1_000, leaseUntil: now + 2_000 });
    if (!first || !second) throw new Error("expected two deliveries");
    await expect(d.acknowledge("jobs", first.receipt, now + 1_500)).resolves.toBe(false);
    await expect(d.acknowledge("jobs", second.receipt, now + 1_500)).resolves.toBe(true);
  });

  it("parses queue attributes and defaults missing values to zero", async () => {
    const { client, calls } = fakeClient(queueUrl, () => ({
      Attributes: {
        ApproximateNumberOfMessages: "2",
        ApproximateNumberOfMessagesDelayed: "3",
        ApproximateNumberOfMessagesNotVisible: "4",
      },
    }));
    const d = new SqsDialect({ client });
    await expect(d.count("jobs", now)).resolves.toEqual({ ready: 2, delayed: 3, reserved: 4 });
    expect(input(calls[1], GetQueueAttributesCommand).AttributeNames).toEqual([
      "ApproximateNumberOfMessages",
      "ApproximateNumberOfMessagesDelayed",
      "ApproximateNumberOfMessagesNotVisible",
    ]);

    const empty = fakeClient(queueUrl, () => ({}));
    await expect(new SqsDialect({ client: empty.client }).count("jobs", now)).resolves.toEqual({
      ready: 0,
      delayed: 0,
      reserved: 0,
    });

    const partial = fakeClient(queueUrl, () => ({ Attributes: { ApproximateNumberOfMessages: "5" } }));
    await expect(new SqsDialect({ client: partial.client }).count("jobs", now)).resolves.toEqual({
      ready: 5,
      delayed: 0,
      reserved: 0,
    });
  });

  it("is idempotent on close and rejects every later call", async () => {
    const { client } = fakeClient();
    const d = new SqsDialect({ client });
    await d.close();
    await d.close();
    await expect(d.enqueue("jobs", "body", { availableAt: now, now })).rejects.toThrow(/closed/);
    await expect(d.dequeue("jobs", { now, leaseUntil: now })).rejects.toThrow(/closed/);
    await expect(d.acknowledge("jobs", receipt("id:handle"), now)).rejects.toThrow(/closed/);
    await expect(d.release("jobs", receipt("id:handle"), { availableAt: now, now })).rejects.toThrow(/closed/);
    await expect(d.count("jobs", now)).rejects.toThrow(/closed/);
  });

  it("bounds the acknowledged receipts it remembers", async () => {
    const calls: unknown[] = [];
    const client = {
      async send(command: unknown): Promise<unknown> {
        calls.push(command);
        if (command instanceof CreateQueueCommand) return queueUrl();
        if (command instanceof ReceiveMessageCommand) {
          return { Messages: [{ MessageId: "held", ReceiptHandle: "held-handle", Body: "body" }] };
        }
        if (command instanceof DeleteMessageCommand) return {};
        throw new Error("unexpected SQS call");
      },
    } as unknown as SqsClient;
    const d = new SqsDialect({ client });
    // A live lease must survive both the acknowledged-tombstone eviction and the retention sweep.
    await expect(d.dequeue("jobs", { now, leaseUntil: now + 30_000 })).resolves.toMatchObject({ id: "held" });
    for (let index = 0; index < 1_025; index += 1) {
      await expect(d.acknowledge("jobs", receipt(`id-${index}:handle-${index}`), now)).resolves.toBe(true);
    }
    expect(calls).toHaveLength(1_027);
    // The oldest acknowledged tombstone was evicted, so a retry of an already-deleted message reaches SQS and
    // reports true. That is the only thing eviction may forget: no live message can be removed this way.
    await expect(d.acknowledge("jobs", receipt("id-0:handle-0"), now)).resolves.toBe(true);
    expect(calls).toHaveLength(1_028);
  });

  it("never forgets a released receipt, even after many acknowledgements", async () => {
    const calls: unknown[] = [];
    const client = {
      async send(command: unknown): Promise<unknown> {
        calls.push(command);
        if (command instanceof CreateQueueCommand) return queueUrl();
        if (command instanceof ReceiveMessageCommand) {
          return { Messages: [{ MessageId: "one", ReceiptHandle: "first", Body: "body" }] };
        }
        if (command instanceof ChangeMessageVisibilityCommand) return {};
        if (command instanceof DeleteMessageCommand) return {};
        throw new Error("unexpected SQS call");
      },
    } as unknown as SqsClient;
    const d = new SqsDialect({ client });
    const message = await d.dequeue("jobs", { now, leaseUntil: now + 30_000 });
    if (!message) throw new Error("expected a message");
    await expect(d.release("jobs", message.receipt, { availableAt: now + 1_000, now })).resolves.toBe(true);

    for (let index = 0; index < 1_025; index += 1) {
      await expect(d.acknowledge("jobs", receipt(`id-${index}:handle-${index}`), now)).resolves.toBe(true);
    }
    const deletes = calls.filter((call) => call instanceof DeleteMessageCommand).length;
    await expect(d.acknowledge("jobs", message.receipt, now)).resolves.toBe(false);
    expect(calls.filter((call) => call instanceof DeleteMessageCommand)).toHaveLength(deletes);
  });

  it("drops settled state older than the SQS retention window", async () => {
    let clock = 1_000_000;
    const spy = vi.spyOn(Date, "now").mockImplementation(() => clock);
    const calls: unknown[] = [];
    const client = {
      async send(command: unknown): Promise<unknown> {
        calls.push(command);
        if (command instanceof CreateQueueCommand) return queueUrl();
        if (command instanceof ReceiveMessageCommand) {
          return { Messages: [{ MessageId: "one", ReceiptHandle: "first", Body: "body" }] };
        }
        if (command instanceof ChangeMessageVisibilityCommand) return {};
        if (command instanceof DeleteMessageCommand) return {};
        throw new Error("unexpected SQS call");
      },
    } as unknown as SqsClient;
    try {
      const d = new SqsDialect({ client });
      const message = await d.dequeue("jobs", { now: clock, leaseUntil: clock + 1_000 });
      if (!message) throw new Error("expected a message");
      await expect(d.release("jobs", message.receipt, { availableAt: clock + 1_000, now: clock })).resolves.toBe(true);

      clock += 15 * 24 * 60 * 60 * 1_000;
      for (let index = 0; index < 1_024; index += 1) {
        await expect(d.acknowledge("jobs", receipt(`id-${index}:handle-${index}`), clock)).resolves.toBe(true);
      }
      // The periodic sweep removed the released state; its receipt is unknown again and reaches SQS.
      const deletes = calls.filter((call) => call instanceof DeleteMessageCommand).length;
      await expect(d.acknowledge("jobs", message.receipt, clock)).resolves.toBe(true);
      expect(calls.filter((call) => call instanceof DeleteMessageCommand)).toHaveLength(deletes + 1);
    } finally {
      spy.mockRestore();
    }
  });

  it("lets only one concurrent acknowledgement of a receipt proceed", async () => {
    let releaseDelete!: () => void;
    const gate = new Promise<void>((resolve) => {
      releaseDelete = resolve;
    });
    let deletes = 0;
    const client = {
      async send(command: unknown): Promise<unknown> {
        if (command instanceof CreateQueueCommand) return queueUrl();
        if (command instanceof ReceiveMessageCommand) {
          return { Messages: [{ MessageId: "one", ReceiptHandle: "handle", Body: "body" }] };
        }
        if (command instanceof DeleteMessageCommand) {
          deletes += 1;
          await gate;
          return {};
        }
        throw new Error("unexpected SQS call");
      },
    } as unknown as SqsClient;
    const d = new SqsDialect({ client });
    const message = await d.dequeue("jobs", { now, leaseUntil: now + 30_000 });
    if (!message) throw new Error("expected a message");

    const first = d.acknowledge("jobs", message.receipt, now);
    await vi.waitFor(() => expect(deletes).toBe(1));
    await expect(d.acknowledge("jobs", message.receipt, now)).resolves.toBe(false);
    releaseDelete();
    await expect(first).resolves.toBe(true);
    expect(deletes).toBe(1);
  });

  it("restores a live lease when an acknowledgement fails", async () => {
    let fail = true;
    const client = {
      async send(command: unknown): Promise<unknown> {
        if (command instanceof CreateQueueCommand) return queueUrl();
        if (command instanceof ReceiveMessageCommand) {
          return { Messages: [{ MessageId: "one", ReceiptHandle: "handle", Body: "body" }] };
        }
        if (command instanceof DeleteMessageCommand) {
          if (fail) {
            fail = false;
            throw sqsError("AccessDenied");
          }
          return {};
        }
        throw new Error("unexpected SQS call");
      },
    } as unknown as SqsClient;
    const d = new SqsDialect({ client });
    const message = await d.dequeue("jobs", { now, leaseUntil: now + 30_000 });
    if (!message) throw new Error("expected a message");

    await expect(d.acknowledge("jobs", message.receipt, now)).rejects.toThrow("AccessDenied");
    await expect(d.acknowledge("jobs", message.receipt, now)).resolves.toBe(true);
  });

  it("forgets a settling state for an unknown receipt when the call fails", async () => {
    let deletes = 0;
    const client = {
      async send(command: unknown): Promise<unknown> {
        if (command instanceof CreateQueueCommand) return queueUrl();
        if (command instanceof DeleteMessageCommand) {
          deletes += 1;
          if (deletes === 1) throw sqsError("AccessDenied");
          return {};
        }
        throw new Error("unexpected SQS call");
      },
    } as unknown as SqsClient;
    const d = new SqsDialect({ client });

    await expect(d.acknowledge("jobs", receipt("remote:handle"), now)).rejects.toThrow("AccessDenied");
    await expect(d.acknowledge("jobs", receipt("remote:handle"), now)).resolves.toBe(true);
    expect(deletes).toBe(2);
  });

  it("returns false when the lease expires between the last check and the acknowledgement claim", async () => {
    const calls: unknown[] = [];
    const client = {
      async send(command: unknown): Promise<unknown> {
        calls.push(command);
        if (command instanceof CreateQueueCommand) return queueUrl();
        if (command instanceof ReceiveMessageCommand) {
          return { Messages: [{ MessageId: "one", ReceiptHandle: "handle", Body: "body" }] };
        }
        throw new Error("unexpected SQS call");
      },
    } as unknown as SqsClient;
    const d = new SqsDialect({ client });
    const before = Date.now();
    const message = await d.dequeue("jobs", { now: before, leaseUntil: before + 1_000 });
    if (!message) throw new Error("expected a message");

    const spy = vi.spyOn(Date, "now");
    try {
      spy
        .mockReturnValueOnce(before)
        .mockReturnValueOnce(before)
        .mockReturnValueOnce(before + 60_000);
      await expect(d.acknowledge("jobs", message.receipt, before)).resolves.toBe(false);
    } finally {
      spy.mockRestore();
    }
    expect(calls.some((call) => call instanceof DeleteMessageCommand)).toBe(false);
  });

  it("returns false when the lease expires between the last check and the release claim", async () => {
    const calls: unknown[] = [];
    const client = {
      async send(command: unknown): Promise<unknown> {
        calls.push(command);
        if (command instanceof CreateQueueCommand) return queueUrl();
        if (command instanceof ReceiveMessageCommand) {
          return { Messages: [{ MessageId: "one", ReceiptHandle: "handle", Body: "body" }] };
        }
        throw new Error("unexpected SQS call");
      },
    } as unknown as SqsClient;
    const d = new SqsDialect({ client });
    const before = Date.now();
    const message = await d.dequeue("jobs", { now: before, leaseUntil: before + 1_000 });
    if (!message) throw new Error("expected a message");

    const spy = vi.spyOn(Date, "now");
    try {
      spy
        .mockReturnValueOnce(before)
        .mockReturnValueOnce(before)
        .mockReturnValueOnce(before + 60_000);
      await expect(d.release("jobs", message.receipt, { availableAt: before + 2_000, now: before })).resolves.toBe(
        false,
      );
    } finally {
      spy.mockRestore();
    }
    expect(calls.some((call) => call instanceof ChangeMessageVisibilityCommand)).toBe(false);
  });

  it("rechecks the lease after creating the queue before changing visibility", async () => {
    const calls: unknown[] = [];
    const client = {
      async send(command: unknown): Promise<unknown> {
        calls.push(command);
        if (command instanceof CreateQueueCommand) return queueUrl();
        if (command instanceof ReceiveMessageCommand) {
          return { Messages: [{ MessageId: "one", ReceiptHandle: "handle", Body: "body" }] };
        }
        throw new Error("unexpected SQS call");
      },
    } as unknown as SqsClient;
    const d = new SqsDialect({ client });
    const before = Date.now();
    const message = await d.dequeue("jobs", { now: before, leaseUntil: before + 1_000 });
    if (!message) throw new Error("expected a message");

    const spy = vi.spyOn(Date, "now");
    try {
      spy.mockReturnValueOnce(before).mockReturnValueOnce(before + 60_000);
      await expect(d.release("jobs", message.receipt, { availableAt: before + 2_000, now: before })).resolves.toBe(
        false,
      );
    } finally {
      spy.mockRestore();
    }
    expect(calls.some((call) => call instanceof ChangeMessageVisibilityCommand)).toBe(false);
  });

  it("rechecks the lease after creating the queue and before deleting", async () => {
    const calls: unknown[] = [];
    const client = {
      async send(command: unknown): Promise<unknown> {
        calls.push(command);
        if (command instanceof CreateQueueCommand) return queueUrl();
        if (command instanceof ReceiveMessageCommand) {
          return { Messages: [{ MessageId: "one", ReceiptHandle: "handle", Body: "body" }] };
        }
        throw new Error("unexpected SQS call");
      },
    } as unknown as SqsClient;
    const d = new SqsDialect({ client });
    const before = Date.now();
    const message = await d.dequeue("jobs", { now: before, leaseUntil: before + 1_000 });
    if (!message) throw new Error("expected a message");

    const spy = vi.spyOn(Date, "now");
    try {
      spy.mockReturnValueOnce(before).mockReturnValueOnce(before + 60_000);
      await expect(d.acknowledge("jobs", message.receipt, before)).resolves.toBe(false);
    } finally {
      spy.mockRestore();
    }
    expect(calls.some((call) => call instanceof DeleteMessageCommand)).toBe(false);
  });
});
