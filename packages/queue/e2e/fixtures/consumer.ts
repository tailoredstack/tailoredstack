// Runs as a consumer would: imports the installed package, never the source tree.

import { InvalidArgumentError, Job, MemoryDialect, Queue } from "@tailoredstack/queue";
import { EffectQueue } from "@tailoredstack/queue/effect";
import { Effect } from "effect";

const root = import.meta.resolve("@tailoredstack/queue");
const effect = import.meta.resolve("@tailoredstack/queue/effect");
if (!root.includes("/node_modules/@tailoredstack/queue/dist/")) throw new Error(`root resolved to ${root}`);
if (!effect.includes("/node_modules/@tailoredstack/queue/dist/")) throw new Error(`effect resolved to ${effect}`);

class Ping extends Job<{ readonly n: number }> {
  declare readonly __jobKey: "Ping";

  async handle(): Promise<void> {}
}

const queue = new EffectQueue(new Queue(new MemoryDialect(), { jobs: { Ping }, now: () => 1_000 }));

const program = Effect.gen(function* () {
  const id = yield* queue.enqueue(new Ping({ n: 1 }));
  const message = yield* queue.dequeue(1_000);
  if (message === null) throw new Error("expected a message");
  const acknowledged = yield* queue.acknowledge(message.receipt);
  const count = yield* queue.count();
  const invalid = yield* Effect.flip(queue.enqueue(new Ping({ n: 2 }), { delay: -1 }));
  if (!(invalid instanceof InvalidArgumentError)) throw new Error("expected InvalidArgumentError");
  return { id, acknowledged, count, invalid: invalid.name };
});

const result = await Effect.runPromise(program);
if (result.count.ready !== 0 || result.count.delayed !== 0 || result.count.reserved !== 0) {
  throw new Error(`unexpected count ${JSON.stringify(result.count)}`);
}
if (result.invalid !== "InvalidArgumentError") throw new Error(`unexpected domain error ${result.invalid}`);
