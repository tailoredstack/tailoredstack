import { MemoryStore } from "../src/drivers/memory.ts";
import { actorSuite } from "./actors-suite.ts";

actorSuite("MemoryStore", (clock) => new MemoryStore({ now: clock.now }));
