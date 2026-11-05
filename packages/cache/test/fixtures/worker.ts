// Child process: runs one Cache operation against a shared Redis prefix and reports JSON on stdout.
import { Redis } from "ioredis";
import { Cache, RedisStore } from "../../src/index.ts";

const [, , url, prefix, op, arg] = process.argv as [string, string, string, string, string, string];
const client = new Redis(url, { maxRetriesPerRequest: 1, lazyConnect: false });
const cache = new Cache(new RedisStore({ client, prefix }));
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function main(): Promise<unknown> {
  switch (op) {
    case "lock-hold": {
      // Acquire, record entry/exit timestamps in Redis, hold for `arg` ms, release.
      return cache.withoutOverlapping(["job"], async () => {
        const t0 = Date.now();
        await client.rpush(`${prefix}trace`, `enter:${process.pid}:${t0}`);
        await sleep(Number(arg));
        await client.rpush(`${prefix}trace`, `exit:${process.pid}:${Date.now()}`);
        return "held";
      }, { lockFor: 5_000, waitFor: 10_000 });
    }
    case "lock-crash": {
      // Take the lock and hang forever inside it. The parent SIGKILLs us: a real crash, no release.
      void cache.withoutOverlapping(["job"], () => new Promise(() => {}), { lockFor: Number(arg), waitFor: 0 });
      await client.set(`${prefix}crashed`, String(process.pid));
      return new Promise(() => {});
    }
    case "lock-try": {
      return cache.withoutOverlapping(["job"], () => "got-it", { waitFor: 0 });
    }
    case "funnel-hold": {
      return cache.funnel(["api"], { limit: 2, waitFor: 10_000, lockFor: 5_000 }, async () => {
        await client.incr(`${prefix}active`);
        const now = Number(await client.get(`${prefix}active`));
        await client.rpush(`${prefix}peak`, String(now));
        await sleep(Number(arg));
        await client.decr(`${prefix}active`);
        return "ok";
      });
    }
    default:
      throw new Error(`unknown op ${op}`);
  }
}

main()
  .then((result) => {
    process.stdout.write(JSON.stringify({ ok: true, result }) + "\n");
    client.disconnect();
  })
  .catch((err: unknown) => {
    process.stdout.write(JSON.stringify({ ok: false, error: err instanceof Error ? err.name : String(err) }) + "\n");
    client.disconnect();
    process.exitCode = 1;
  });
