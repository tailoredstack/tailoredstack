import { MemoryStore } from "../src/drivers/memory.ts";
import { storeContract } from "../src/testing/index.ts";

storeContract("MemoryStore", (clock) => new MemoryStore({ now: clock.now }));
