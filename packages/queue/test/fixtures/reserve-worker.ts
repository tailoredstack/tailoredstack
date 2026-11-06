// Child process for the cross-process reserve test. Usage: node reserve-worker.ts <dbPath> <queue>
// Opens its own SqliteQueueDriver, prints "ready", waits for a "go" line on stdin, then reserves until the
// queue is empty and prints each reserved id as one JSON line.
import { createInterface } from "node:readline";
import { SqliteQueueDriver } from "../../src/drivers/sqlite.ts";

const [path, queue] = process.argv.slice(2);
if (!path || !queue) throw new Error("usage: reserve-worker.ts <dbPath> <queue>");

const driver = new SqliteQueueDriver({ path });
const lines = createInterface({ input: process.stdin });

console.log("ready");
lines.once("line", async () => {
  lines.close();
  for (;;) {
    const job = await driver.reserve(queue, 1_000, Number.MAX_SAFE_INTEGER);
    if (!job) break;
    console.log(JSON.stringify(job.id));
    await new Promise((r) => setTimeout(r, 2)); // pause so the other child gets turns
  }
  process.stdin.destroy(); // release stdin so the child exits when the queue is drained
});
