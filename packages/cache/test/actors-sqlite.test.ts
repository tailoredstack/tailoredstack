import { SqliteStore } from "../src/drivers/sqlite.ts";
import { actorSuite } from "./actors-suite.ts";

actorSuite("SqliteStore", (clock) => new SqliteStore({ path: ":memory:", now: clock.now }));
