import { createHash, randomUUID } from "node:crypto";
import {
  ChangeMessageVisibilityCommand,
  CreateQueueCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import { GenericContainer, type StartedTestContainer } from "testcontainers";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { createSqsDriver } from "../src/drivers/sqs.js";
import { LimitError } from "../src/errors.ts";
import type { Driver } from "../src/types.js";
import { driverContract } from "./driver-contract.ts";

const T0 = 1_000_000;

describe("sqs lease ownership", () => {
  it("keeps a live lease hidden from a peer that reserves after SQS makes its copy visible", async () => {
    const prefix = freshPrefix();
    const driver = createSqsDriver({ client: connect(), queueNamePrefix: prefix });
    const peer = createSqsDriver({ client: connect(), queueNamePrefix: prefix });
    await driver.push("jobs", "body", { availableAt: T0, now: T0 });
    const lease = await driver.reserve("jobs", T0, T0 + 60_000);
    if (lease === null) throw new Error("expected a lease");
    // The lease lives in the message body, so a peer that sees the copy visible still finds it leased on the caller's clock.
    await expect(peer.reserve("jobs", T0 + 1, T0 + 60_000)).resolves.toBeNull();
    await expect(driver.delete("jobs", lease.token, T0 + 1)).resolves.toBe(true);
  });

  // Best-effort I9: standard SQS can deliver one message to two concurrent receivers, so a race may lease a message
  // on two instances. The race must still never lose a message.
  it("loses no message when two instances race for them (cross-instance I9 is best-effort)", async () => {
    const prefix = freshPrefix();
    const first = createSqsDriver({ client: connect(), queueNamePrefix: prefix });
    const second = createSqsDriver({ client: connect(), queueNamePrefix: prefix });
    for (let i = 0; i < 10; i++) {
      await first.push("jobs", `m${i}`, { availableAt: T0, now: T0 });
    }
    const claim = async (driver: Driver): Promise<string[]> => {
      const got: string[] = [];
      for (let i = 0; i < 10; i++) {
        const lease = await driver.reserve("jobs", T0, T0 + 60_000);
        if (lease !== null) got.push(lease.payload);
      }
      return got;
    };
    const [a, b] = await Promise.all([claim(first), claim(second)]);
    const expected = Array.from({ length: 10 }, (_, i) => `m${i}`);
    expect(new Set([...a, ...b])).toEqual(new Set(expected));
  });

  it("lets a peer re-reserve a message whose lease ended on the caller's clock", async () => {
    const prefix = freshPrefix();
    const first = createSqsDriver({ client: connect(), queueNamePrefix: prefix });
    const second = createSqsDriver({ client: connect(), queueNamePrefix: prefix });
    await first.push("jobs", "body", { availableAt: T0, now: T0 });
    const lease = await first.reserve("jobs", T0, T0 + 60_000);
    if (lease === null) throw new Error("expected the first instance to lease the message");
    // The lease ends on the caller's clock at T0 + 60 s; SQS's own visibility for the message is still 60 s of real time.
    const again = await second.reserve("jobs", T0 + 60_000, T0 + 120_000);
    if (again === null) throw new Error("expected the peer to re-reserve the message");
    expect(again.payload).toBe("body");
    expect(again.attempts).toBe(2);
    // The first lease's token no longer names a copy, so its delete is rejected.
    await expect(first.delete("jobs", lease.token, T0 + 60_000)).resolves.toBe(false);
  });

  // R1 amendment: equal availableAt is ordered by push sequence within one instance. Across instances the order is unspecified, so this covers one instance only.
  it("keeps push order within one instance", async () => {
    const prefix = freshPrefix();
    const driver = createSqsDriver({ client: connect(), queueNamePrefix: prefix });
    await driver.push("jobs", "a1", { availableAt: T0, now: T0 });
    await driver.push("jobs", "a2", { availableAt: T0, now: T0 });
    await driver.push("jobs", "a3", { availableAt: T0, now: T0 });
    const order: string[] = [];
    for (let i = 0; i < 3; i++) {
      const lease = await driver.reserve("jobs", T0, T0 + 60_000);
      if (lease === null) throw new Error("expected a message");
      order.push(lease.payload);
      await driver.delete("jobs", lease.token, T0);
    }
    expect(order).toEqual(["a1", "a2", "a3"]);
  });

  it("keeps only the newest copy when an older copy of the same message is still visible", async () => {
    const prefix = freshPrefix();
    const client = connect();
    const digest = createHash("sha256").update("jobs").digest("hex").slice(0, 32);
    const { QueueUrl } = await client.send(new CreateQueueCommand({ QueueName: `${prefix}-${digest}` }));
    const queueUrl = QueueUrl as string;
    // A failed rewrite can leave the old copy visible beside its replacement; the driver must never choose the old one.
    const body = (id: string, version: number, payload: string, pushSeq: number): string =>
      JSON.stringify({
        id,
        payload: Buffer.from(payload).toString("base64"),
        pushSeq,
        version,
        availableAt: 0,
        attempts: 0,
        leaseUntil: 0,
        token: null,
      });
    for (const message of [
      body("x", 0, "old-x", 1),
      body("x", 1, "new-x", 1),
      body("y", 1, "new-y", 2),
      body("y", 0, "old-y", 2),
    ]) {
      await client.send(new SendMessageCommand({ QueueUrl: queueUrl, MessageBody: message }));
    }
    const driver = createSqsDriver({ client, queueNamePrefix: prefix });
    await expect(driver.size("jobs", 0)).resolves.toEqual({ ready: 2, delayed: 0, reserved: 0 });
    const first = await driver.reserve("jobs", 0, 1_000);
    const second = await driver.reserve("jobs", 0, 1_000);
    expect([first?.payload, second?.payload].sort()).toEqual(["new-x", "new-y"]);
    await expect(driver.reserve("jobs", 0, 1_000)).resolves.toBeNull();
  });
});

describe("sqs message limit", () => {
  it("rejects a push whose stored envelope exceeds the SQS message limit, and stores nothing", async () => {
    const driver = createSqsDriver({ client: connect(), queueNamePrefix: freshPrefix() });
    // 200 000 raw bytes is within MAX_PAYLOAD_BYTES, but base64 grows it to about 266 700 bytes, over SQS's 262 144.
    await expect(driver.push("jobs", "a".repeat(200_000), { availableAt: T0, now: T0 })).rejects.toThrow(LimitError);
    await expect(driver.size("jobs", T0)).resolves.toEqual({ ready: 0, delayed: 0, reserved: 0 });
  });
  it("rejects a push whose envelope would exceed the limit once a reserve rewrites it, and stores nothing", async () => {
    const driver = createSqsDriver({ client: connect(), queueNamePrefix: freshPrefix() });
    // 196 497 raw bytes still fit in one message as pushed, but a reserve adds the token and lease fields and pushes the envelope over 262 144.
    await expect(driver.push("jobs", "a".repeat(196_497), { availableAt: T0, now: T0 })).rejects.toThrow(LimitError);
    await expect(driver.size("jobs", T0)).resolves.toEqual({ ready: 0, delayed: 0, reserved: 0 });
  });
});

// Each SQS call may wait up to 1 s for a long poll; the contract's empty reserves take that long.
vi.setConfig({ testTimeout: 60_000 });

const ELASTICMQ_PORT = 9324;
let container: StartedTestContainer;
let endpoint: string;
const clients: SQSClient[] = [];

function connect(): SQSClient {
  const client = new SQSClient({
    endpoint,
    region: "elasticmq",
    credentials: { accessKeyId: "x", secretAccessKey: "x" },
  });
  clients.push(client);
  return client;
}

// Each test gets its own namespace, so queues never leak between tests.
function freshPrefix(): string {
  return `t${randomUUID().replaceAll("-", "").slice(0, 12)}`;
}

beforeAll(async () => {
  container = await new GenericContainer("softwaremill/elasticmq-native:latest")
    .withExposedPorts(ELASTICMQ_PORT)
    .start();
  endpoint = `http://${container.getHost()}:${container.getMappedPort(ELASTICMQ_PORT)}`;
}, 120_000);

afterAll(async () => {
  for (const client of clients.splice(0)) {
    client.destroy();
  }
  await container.stop();
});

// Two driver instances, each with its own SDK client against one SQS endpoint: the shared-backend contract (B7) needs a peer.
driverContract({
  name: "sqs",
  capabilities: { shared: true, exactCounts: false, maxPushDelayMs: 900_000 },
  // Largest raw payload whose envelope still fits 262144 bytes after a reserve rewrites it at T0 (1_000_000). Derived by measuring the envelope with every counter at MAX_SAFE_INTEGER and a 36-character token.
  maxPayloadBytes: 196_440,
  fixture: async () => {
    const prefix = freshPrefix();
    return {
      driver: createSqsDriver({ client: connect(), queueNamePrefix: prefix }),
      peer: createSqsDriver({ client: connect(), queueNamePrefix: prefix }),
    };
  },
});

// A client whose next call of one SDK command class fails with the given error; every other call goes through.
function injectable(): { client: SQSClient; arm: (command: new (...args: never[]) => object, failure: Error) => void } {
  const client = connect();
  const send = client.send.bind(client);
  let armed: { command: new (...args: never[]) => object; failure: Error } | undefined;
  client.send = ((command: object) => {
    if (armed !== undefined && command instanceof armed.command) {
      const { failure } = armed;
      armed = undefined;
      return Promise.reject(failure);
    }
    return send(command as never);
  }) as typeof client.send;
  return {
    client,
    arm: (command, failure) => {
      armed = { command, failure };
    },
  };
}

// The error SQS returns when a receipt handle no longer names the current copy.
function staleReceipt(): Error {
  return Object.assign(new Error("receipt is stale"), { name: "ReceiptHandleIsInvalid" });
}

describe("sqs driver backend faults", () => {
  it("propagates a CreateQueue failure unchanged", async () => {
    const failure = new Error("create refused");
    const { client, arm } = injectable();
    arm(CreateQueueCommand, failure);
    const driver = createSqsDriver({ client, queueNamePrefix: freshPrefix() });
    await expect(driver.push("jobs", "body", { availableAt: 0, now: 0 })).rejects.toBe(failure);
  });

  it("propagates a SendMessage failure on push unchanged", async () => {
    const failure = new Error("send refused");
    const { client, arm } = injectable();
    const driver = createSqsDriver({ client, queueNamePrefix: freshPrefix() });
    await driver.push("jobs", "warm-up", { availableAt: 0, now: 0 });
    arm(SendMessageCommand, failure);
    await expect(driver.push("jobs", "body", { availableAt: 0, now: 0 })).rejects.toBe(failure);
  });

  it("propagates a ReceiveMessage failure on reserve unchanged", async () => {
    const failure = new Error("receive refused");
    const prefix = freshPrefix();
    const seeded = createSqsDriver({ client: connect(), queueNamePrefix: prefix });
    await seeded.push("jobs", "body", { availableAt: 0, now: 0 });
    const { client, arm } = injectable();
    const driver = createSqsDriver({ client, queueNamePrefix: prefix });
    arm(ReceiveMessageCommand, failure);
    await expect(driver.reserve("jobs", 0, 1_000)).rejects.toBe(failure);
  });

  it("propagates a ChangeMessageVisibility failure on reserve unchanged", async () => {
    const failure = new Error("visibility refused");
    const prefix = freshPrefix();
    const seeded = createSqsDriver({ client: connect(), queueNamePrefix: prefix });
    await seeded.push("jobs", "body", { availableAt: 0, now: 0 });
    await seeded.reserve("jobs", 0, 1_000);
    const { client, arm } = injectable();
    const driver = createSqsDriver({ client, queueNamePrefix: prefix });
    arm(ChangeMessageVisibilityCommand, failure);
    // At 500 the copy is still leased, so reserve restores it rather than choosing it; that restore is the call that fails.
    await expect(driver.reserve("jobs", 500, 2_000)).rejects.toBe(failure);
  });

  it("propagates a DeleteMessage failure on delete unchanged", async () => {
    const failure = new Error("delete refused");
    const prefix = freshPrefix();
    const { client, arm } = injectable();
    const driver = createSqsDriver({ client, queueNamePrefix: prefix });
    await driver.push("jobs", "body", { availableAt: 0, now: 0 });
    const lease = await driver.reserve("jobs", 0, 1_000);
    if (lease === null) {
      throw new Error("expected a lease");
    }
    arm(DeleteMessageCommand, failure);
    await expect(driver.delete("jobs", lease.token, 0)).rejects.toBe(failure);
  });

  it("propagates a SendMessage failure on release unchanged", async () => {
    const failure = new Error("release send refused");
    const prefix = freshPrefix();
    const { client, arm } = injectable();
    const driver = createSqsDriver({ client, queueNamePrefix: prefix });
    await driver.push("jobs", "body", { availableAt: 0, now: 0 });
    const lease = await driver.reserve("jobs", 0, 1_000);
    if (lease === null) {
      throw new Error("expected a lease");
    }
    arm(SendMessageCommand, failure);
    await expect(driver.release("jobs", lease.token, 0, 0)).rejects.toBe(failure);
  });

  it("propagates a DeleteMessage failure on release unchanged", async () => {
    const failure = new Error("release delete refused");
    const prefix = freshPrefix();
    const { client, arm } = injectable();
    const driver = createSqsDriver({ client, queueNamePrefix: prefix });
    await driver.push("jobs", "body", { availableAt: 0, now: 0 });
    const lease = await driver.reserve("jobs", 0, 1_000);
    if (lease === null) {
      throw new Error("expected a lease");
    }
    arm(DeleteMessageCommand, failure);
    await expect(driver.release("jobs", lease.token, 0, 0)).rejects.toBe(failure);
  });

  it("propagates a ChangeMessageVisibility failure on size unchanged", async () => {
    const failure = new Error("size visibility refused");
    const prefix = freshPrefix();
    const seeded = createSqsDriver({ client: connect(), queueNamePrefix: prefix });
    await seeded.push("jobs", "body", { availableAt: 0, now: 0 });
    const { client, arm } = injectable();
    const driver = createSqsDriver({ client, queueNamePrefix: prefix });
    arm(ChangeMessageVisibilityCommand, failure);
    // size restores every copy it drains, so the restore call is the one that fails.
    await expect(driver.size("jobs", 0)).rejects.toBe(failure);
  });

  it("propagates a ReceiveMessage failure on size unchanged", async () => {
    const failure = new Error("size receive refused");
    const { client, arm } = injectable();
    const driver = createSqsDriver({ client, queueNamePrefix: freshPrefix() });
    arm(ReceiveMessageCommand, failure);
    await expect(driver.size("jobs", 0)).rejects.toBe(failure);
  });

  it("ignores a stale receipt when release finds its replaced copy already gone", async () => {
    const prefix = freshPrefix();
    const seeded = createSqsDriver({ client: connect(), queueNamePrefix: prefix });
    await seeded.push("jobs", "body", { availableAt: 0, now: 0 });
    const lease = await seeded.reserve("jobs", 0, 1_000);
    if (lease === null) throw new Error("expected a lease");
    const { client, arm } = injectable();
    const driver = createSqsDriver({ client, queueNamePrefix: prefix });
    arm(DeleteMessageCommand, staleReceipt());
    await expect(driver.release("jobs", lease.token, 0, 0)).resolves.toBe(true);
  });

  it("ignores a stale receipt when reserve restores a copy that is already gone", async () => {
    const prefix = freshPrefix();
    const seeded = createSqsDriver({ client: connect(), queueNamePrefix: prefix });
    await seeded.push("jobs", "body", { availableAt: 0, now: 0 });
    await seeded.reserve("jobs", 0, 1_000);
    const { client, arm } = injectable();
    const driver = createSqsDriver({ client, queueNamePrefix: prefix });
    arm(ChangeMessageVisibilityCommand, staleReceipt());
    // At 500 the copy is still leased, so reserve restores it; the restore fails as stale and is ignored.
    await expect(driver.reserve("jobs", 500, 2_000)).resolves.toBeNull();
  });

  it("reports delete as false when SQS rejects the receipt as stale, because the message was not removed", async () => {
    const prefix = freshPrefix();
    const seeded = createSqsDriver({ client: connect(), queueNamePrefix: prefix });
    await seeded.push("jobs", "body", { availableAt: 0, now: 0 });
    const lease = await seeded.reserve("jobs", 0, 1_000);
    if (lease === null) throw new Error("expected a lease");
    const { client, arm } = injectable();
    const driver = createSqsDriver({ client, queueNamePrefix: prefix });
    arm(DeleteMessageCommand, staleReceipt());
    await expect(driver.delete("jobs", lease.token, 0)).resolves.toBe(false);
  });

  it("reads a receive reply without a Messages field as an empty batch", async () => {
    // SQS omits Messages when nothing is visible; ElasticMQ returns an empty list, so this stubs the omitted form.
    const client = connect();
    const send = client.send.bind(client);
    client.send = (async (command: object) =>
      command instanceof ReceiveMessageCommand ? {} : send(command as never)) as typeof client.send;
    const driver = createSqsDriver({ client, queueNamePrefix: freshPrefix() });
    await expect(driver.reserve("jobs", 0, 1_000)).resolves.toBeNull();
    await expect(driver.size("jobs", 0)).resolves.toEqual({ ready: 0, delayed: 0, reserved: 0 });
  });
});
