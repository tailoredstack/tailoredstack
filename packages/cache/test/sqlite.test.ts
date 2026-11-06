import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { Cache } from "../src/cache.ts";
import { SqliteStore } from "../src/drivers/sqlite.ts";
import { storeContract } from "../src/testing/index.ts";

storeContract("SqliteStore", (clock) => new SqliteStore({ path: ":memory:", now: clock.now }));

// Failures are injected with triggers on a real file DB, since the driver's prepared statements are private.
describe("SqliteStore failure paths", () => {
  let dir: string;
  let path: string;
  let admin: DatabaseSync | undefined;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "cache-sqlite-"));
    path = join(dir, "cache.db");
  });

  afterEach(() => {
    admin?.close();
    admin = undefined;
    rmSync(dir, { recursive: true, force: true });
  });

  it("a write that fails mid-increment rolls back: value unchanged, transaction closed", async () => {
    const store = new SqliteStore({ path });
    const cache = new Cache(store);
    try {
      await cache.set(["n"], 5);
      admin = new DatabaseSync(path);
      admin.exec(
        "CREATE TRIGGER fail_inc BEFORE UPDATE ON cache WHEN NEW.value = '6' BEGIN SELECT RAISE(ABORT, 'disk full'); END;",
      );
      await expect(cache.increment(["n"])).rejects.toThrow("disk full");
      admin.exec("DROP TRIGGER fail_inc");

      expect(await cache.get(["n"])).toBe(5);
      // Without ROLLBACK, BEGIN IMMEDIATE here fails: "cannot start a transaction within a transaction".
      expect(await cache.increment(["n"])).toBe(6);
    } finally {
      store.close();
    }
  });

  it("a delete that fails mid-invalidate keeps the failed and later rows; a retry finishes", async () => {
    const store = new SqliteStore({ path });
    const cache = new Cache(store);
    try {
      await cache.set(["u", "a"], 1);
      await cache.set(["u", "b"], 2);
      await cache.set(["u", "c"], 3);
      admin = new DatabaseSync(path);
      admin.exec(
        `CREATE TRIGGER fail_del BEFORE DELETE ON cache WHEN OLD.key LIKE '%"b"%' BEGIN SELECT RAISE(ABORT, 'disk full'); END;`,
      );
      await expect(cache.invalidate(["u"])).rejects.toThrow("disk full");
      expect(await cache.get(["u", "b"])).toBe(2);
      expect(await cache.get(["u", "c"])).toBe(3);

      admin.exec("DROP TRIGGER fail_del");
      expect(await cache.invalidate(["u"])).toBe(2);
      expect(await cache.get(["u", "b"])).toBeUndefined();
    } finally {
      store.close();
    }
  });

  it("using a cache after its store is closed rejects, including scoped caches", async () => {
    const store = new SqliteStore({ path: ":memory:" });
    const cache = new Cache(store);
    const scoped = cache.prefix(["u"]).ttl(1000);
    await cache.set(["k"], 1);
    store.close();
    await expect(cache.get(["k"])).rejects.toThrow();
    await expect(cache.set(["k"], 2)).rejects.toThrow();
    await expect(scoped.get(["k"])).rejects.toThrow();
    await expect(scoped.set(["k"], 2)).rejects.toThrow();
  });
});
