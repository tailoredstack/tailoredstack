import { describe, expect, it } from "vitest";
import { SqliteStore } from "../src/drivers/sqlite.ts";
import { encodeKey } from "../src/keys.ts";

describe("SqliteStore prefix query plan", () => {
  it("keysWithPrefix uses the primary-key index, never a full table scan", () => {
    const store = new SqliteStore({ path: ":memory:" });
    for (const prefix of [encodeKey(["user", 1]), `${encodeKey(["user"]).slice(0, -1)},`, "["]) {
      const plan = store.explainKeysWithPrefix(prefix).join("\n");
      expect(plan).toMatch(/USING (COVERING )?INDEX/);
      expect(plan).not.toContain("SCAN cache");
    }
    store.close();
  });
});
