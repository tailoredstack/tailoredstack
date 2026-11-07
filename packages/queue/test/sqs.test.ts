import { randomUUID } from "node:crypto";
import {
  ChangeMessageVisibilityCommand,
  CreateQueueCommand,
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  type ReceiveMessageCommandOutput,
  SendMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import { GenericContainer, type StartedTestContainer } from "testcontainers";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { createSqsDriver } from "../src/sqs.js";
import { driverContract } from "../src/testing/driver-contract.js";
import type { Driver } from "../src/types.js";

const T0 = 1_000_000;

// Wraps the client so a test can see the receipt handles the driver received, and act on them as SQS would in real time.
function recording(): { client: SQSClient; received: { QueueUrl: string; ReceiptHandle: string }[] } {
  const client = connect();
  const send = client.send.bind(client);
  const received: { QueueUrl: string; ReceiptHandle: string }[] = [];
  client.send = (async (command: object) => {
    const reply = await send(command as never);
    if (command instanceof ReceiveMessageCommand) {
      const url = command.input.QueueUrl ?? "";
      for (const message of (reply as ReceiveMessageCommandOutput).Messages ?? []) {
        received.push({ QueueUrl: url, ReceiptHandle: message.ReceiptHandle ?? "" });
      }
    }
    return reply;
  }) as typeof client.send;
  return { client, received };
}

describe("sqs lease ownership", () => {
  it("keeps a live lease hidden when SQS makes its message visible before the lease ends", async () => {
    const { client, received } = recording();
    const driver = createSqsDriver({ client, queueNamePrefix: freshPrefix() });
    await driver.push("jobs", "body", { availableAt: T0, now: T0 });
    const lease = await driver.reserve("jobs", T0, T0 + 60_000);
    if (lease === null) throw new Error("expected a lease");
    const [seen] = received;
    if (seen === undefined) throw new Error("expected a received message");
    // SQS restores visibility on its own clock, while the caller's lease still runs.
    await client.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl: seen.QueueUrl,
        ReceiptHandle: seen.ReceiptHandle,
        VisibilityTimeout: 0,
      }),
    );
    await expect(driver.reserve("jobs", T0 + 1, T0 + 60_000)).resolves.toBeNull();
    await expect(driver.delete("jobs", lease.token, T0 + 1)).resolves.toBe(true);
  });

  it("never hands two instances the same message when they reserve at once", async () => {
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
    expect([...a, ...b].sort()).toEqual(expected);
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
    expect(again?.attempts).toBe(2);
  });

  it("breaks equal availableAt ties by push order across instances", async () => {
    const prefix = freshPrefix();
    const first = createSqsDriver({ client: connect(), queueNamePrefix: prefix });
    const second = createSqsDriver({ client: connect(), queueNamePrefix: prefix });
    await first.push("jobs", "a1", { availableAt: T0, now: T0 });
    await first.push("jobs", "a2", { availableAt: T0, now: T0 });
    await first.push("jobs", "a3", { availableAt: T0, now: T0 });
    await second.push("jobs", "b1", { availableAt: T0, now: T0 });
    const order: string[] = [];
    for (let i = 0; i < 4; i++) {
      const lease = await first.reserve("jobs", T0, T0 + 60_000);
      if (lease === null) throw new Error("expected a message");
      order.push(lease.payload);
      await first.delete("jobs", lease.token, T0);
    }
    expect(order).toEqual(["a1", "a2", "a3", "b1"]);
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
    const { client, arm } = injectable();
    const driver = createSqsDriver({ client, queueNamePrefix: prefix });
    arm(ChangeMessageVisibilityCommand, failure);
    await expect(driver.reserve("jobs", 0, 1_000)).rejects.toBe(failure);
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

  it("propagates a GetQueueAttributes failure on size unchanged", async () => {
    const failure = new Error("attributes refused");
    const { client, arm } = injectable();
    const driver = createSqsDriver({ client, queueNamePrefix: freshPrefix() });
    arm(GetQueueAttributesCommand, failure);
    await expect(driver.size("jobs", 0)).rejects.toBe(failure);
  });

  it("propagates a ReceiveMessage failure on size unchanged", async () => {
    const failure = new Error("size receive refused");
    const { client, arm } = injectable();
    const driver = createSqsDriver({ client, queueNamePrefix: freshPrefix() });
    arm(ReceiveMessageCommand, failure);
    await expect(driver.size("jobs", 0)).rejects.toBe(failure);
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
