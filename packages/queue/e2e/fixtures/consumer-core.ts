// Runs with no `effect` installed: the core entry must not need the optional peer.
import { Job, MemoryDialect, Queue } from "@tailoredstack/queue";

const root = import.meta.resolve("@tailoredstack/queue");
if (!root.includes("/node_modules/@tailoredstack/queue/dist/")) throw new Error(`root resolved to ${root}`);

class Ping extends Job<{ readonly n: number }> {
  declare readonly __jobKey: "Ping";

  async handle(): Promise<void> {}
}

const queue = new Queue(new MemoryDialect(), { jobs: { Ping }, now: () => 1_000 });
const id = await queue.enqueue(new Ping({ n: 1 }));
const message = await queue.dequeue(1_000);
if (message === null) throw new Error("expected a message");
if (!(await queue.acknowledge(message.receipt))) throw new Error("expected acknowledge to succeed");
if (id !== message.id) throw new Error(`unexpected id ${id}`);
