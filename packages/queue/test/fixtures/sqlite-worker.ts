// Child process: claims and acknowledges messages from one shared SQLite file, printing each payload it acks.
import { SqliteDialect } from "../../src/sqlite-dialect.ts";

const [, , dbPath, limit] = process.argv as [string, string, string, string];

// Wait for the parent's "go" so every worker starts its first claim at the same moment. Without it the workers
// start at staggered times and a read-then-upgrade bug could slip through untested.
process.stdin.setEncoding("utf8");
await new Promise<void>((resolve) => {
  process.stdin.once("data", () => resolve());
});
process.stdin.destroy();

const dialect = new SqliteDialect({ path: dbPath });

for (let i = 0; i < Number(limit); i += 1) {
  const now = Date.now();
  const message = await dialect.dequeue("jobs", { now, leaseUntil: now + 30_000 });
  if (!message) break;
  process.stdout.write(`${message.payload}\n`);
  await dialect.acknowledge("jobs", message.receipt, Date.now());
}

await dialect.close();
