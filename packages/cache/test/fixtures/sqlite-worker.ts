// Child process: increments one key in a shared SQLite file `count` times, then exits.
import { SqliteStore } from "../../src/drivers/sqlite.ts";

const [, , dbPath, count] = process.argv as [string, string, string];
const store = new SqliteStore({ path: dbPath });

for (let i = 0; i < Number(count); i++) await store.increment("counter");
