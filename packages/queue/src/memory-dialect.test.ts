import { runDialectSuite } from "./dialect-suite.ts";
import { MemoryDialect } from "./memory-dialect.ts";

runDialectSuite("MemoryDialect", () => new MemoryDialect());
