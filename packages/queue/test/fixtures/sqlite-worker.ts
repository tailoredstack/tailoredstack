// Child process: claims and acknowledges messages from one shared SQLite file, printing each payload it acks.
import { SqliteDialect } from "../../src/sqlite-dialect.ts";

const [, , dbPath, limit] = process.argv as [string, string, string, string];
const dialect = new SqliteDialect({ path: dbPath });

for (let i = 0; i < Number(limit); i += 1) {
  const now = Date.now();
  const message = await dialect.dequeue("jobs", { now, leaseUntil: now + 30_000 });
  if (!message) break;
  process.stdout.write(`${message.payload}\n`);
  await dialect.acknowledge("jobs", message.receipt, Date.now());
}

await dialect.close();
