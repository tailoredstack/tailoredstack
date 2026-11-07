import { randomUUID } from "node:crypto";
import {
  ChangeMessageVisibilityCommand,
  CreateQueueCommand,
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import { GenericContainer, type StartedTestContainer } from "testcontainers";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { createSqsDriver } from "../src/sqs.js";
import { driverContract } from "../src/testing/driver-contract.js";

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
      for (const message of reply.Messages ?? []) {
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

  it("does not lease a message another instance holds when the first instance's lease ends", async () => {
    const prefix = freshPrefix();
    const { client, received } = recording();
    const first = createSqsDriver({ client, queueNamePrefix: prefix });
    const second = createSqsDriver({ client: connect(), queueNamePrefix: prefix });
    await first.push("jobs", "body", { availableAt: T0, now: T0 });
    const lease = await first.reserve("jobs", T0, T0 + 1_000);
    if (lease === null) throw new Error("expected a lease");
    const [seen] = received;
    if (seen === undefined) throw new Error("expected a received message");
    await client.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl: seen.QueueUrl,
        ReceiptHandle: seen.ReceiptHandle,
        VisibilityTimeout: 0,
      }),
    );
    const held = await second.reserve("jobs", T0 + 500, T0 + 60_000);
    if (held === null) throw new Error("expected the second instance to lease the message");
    // The first lease ends on the caller's clock; its old receipt must not free the message the second instance holds.
    await expect(first.reserve("jobs", T0 + 1_000, T0 + 2_000)).resolves.toBeNull();
    await expect(second.delete("jobs", held.token, T0 + 500)).resolves.toBe(true);
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

  it("rejects when the SDK client is destroyed", async () => {
    const client = connect();
    const driver = createSqsDriver({ client, queueNamePrefix: freshPrefix() });
    client.destroy();
    await expect(driver.size("jobs", 0)).rejects.toThrow();
  });
});
