import { SqliteStore } from "../src/drivers/sqlite.ts";
import { storeContract } from "../src/testing/index.ts";

storeContract("SqliteStore", (clock) => new SqliteStore({ path: ":memory:", now: clock.now }));
