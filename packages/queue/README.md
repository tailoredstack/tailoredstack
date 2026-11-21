# @tailoredstack/queue

A typed job queue with pluggable dialects, modeled on Laravel queues. The backend is an injected object, so the core imports no driver and you pay only for the dialect you use.

## Install

```sh
pnpm add @tailoredstack/queue
pnpm add effect         # only if you use @tailoredstack/queue/effect
```

## Dialects

| Dialect         | Storage                       | Ordering           | Exact counts | Delay / lease caps |
| --------------- | ----------------------------- | ------------------ | ------------ | ------------------ |
| `MemoryDialect` | process memory                | earliest due first | yes          | none               |
| `SqliteDialect` | a SQLite file (`node:sqlite`) | earliest due first | yes          | none               |
| `RedisDialect`  | Redis                         | earliest due first | yes          | none               |
| `SqsDialect`    | an SQS queue                  | best effort        | no           | 900 s / 12 h       |

"Earliest due first" means a job becomes claimable at `availableAt` (now, plus any delay) and the dialect serves the smallest `availableAt`; jobs whose delay expires earlier are served before jobs enqueued earlier. Ties keep insertion order. SQS standard queues do not promise that order, so `SqsDialect` declares `ordered: false` instead of sorting messages locally to fake it.

```ts
import { Job, Queue, SqliteDialect } from "@tailoredstack/queue";

class SendEmail extends Job<{ to: string }> {
  declare readonly __jobKey: "sendEmail";

  async handle(): Promise<void> {
    // send the email
  }
}

const queue = new Queue(new SqliteDialect({ path: "./queue.db" }), {
  jobs: { sendEmail: SendEmail },
});

await queue.enqueue(new SendEmail({ to: "ada@example.com" }));

const message = await queue.dequeue(); // lease 30s by default
if (message) await queue.acknowledge(message.receipt);
```

Swap `new SqliteDialect({ path: "./queue.db" })` for `new MemoryDialect()` to run the same code in one process with no file.

### SQS

SQS lives behind its own subpath so the optional `@aws-sdk/client-sqs` peer is only required when you import it. Install both packages and keep a client you own:

```sh
pnpm add @tailoredstack/queue @aws-sdk/client-sqs
```

```ts
import { SQSClient } from "@aws-sdk/client-sqs";
import { Queue } from "@tailoredstack/queue";
import { SqsDialect } from "@tailoredstack/queue/sqs";

const client = new SQSClient({ region: "us-east-1" });
const queue = new Queue(new SqsDialect({ client, queueNamePrefix: "billing" }), {
  jobs: { sendEmail: SendEmail },
});
```

The dialect creates one queue per queue name, named `<prefix>-<sha256(name) first 32 hex>`; the prefix defaults to `queue` and may be at most 47 characters. Supply a different prefix to namespace several applications in one account.

SQS has a backend clock: `now` arguments are ignored and its own seconds-granular clock decides when a delayed or leased message is visible. Delays are capped at 900 s and leases at 12 h. `count()` comes from `GetQueueAttributes` and is approximate; SQS cannot distinguish an in-flight message from one released with a visibility delay, so both are reported in `reserved` instead of `delayed`.

The dialect keeps a per-message, in-process ledger so a stale, settled, or expired-lease receipt returns `false` instead of deleting a message another consumer now holds. A receipt is stale when it is not the latest one this instance handed out for its message. Acknowledged tombstones are bounded (the oldest is forgotten first, which can only let a very late retry of an already-deleted message report `true`). Released and expired state is kept until that message is received or settled again, and swept after a 15-day window, past the 14-day SQS maximum. Receipts from another process, and receipts after a restart, are unknown to the ledger and are passed to SQS directly, so the protection is same-instance and best effort. `close()` does not destroy the client and leaves the queues in place.

The SQLite dialect owns one table in the file, `queue_messages` by default. Pass `table` to namespace several queues inside one database:

```ts
new SqliteDialect({ path: "./app.db", table: "billing_queue_messages" });
```

Index names are derived from the table name (`billing_queue_messages_due`, `billing_queue_messages_receipt`), so dialects with different tables never collide. The name is quoted, so any non-empty name without a NUL byte works; empty names throw `TypeError` before the file is opened.

### Redis

`RedisDialect` takes an injected client, so it never imports `ioredis` itself. `ioredis` stays an optional peer:

```ts
import { Redis } from "ioredis";
import { RedisDialect } from "@tailoredstack/queue";

const url = process.env.REDIS_URL;
if (url === undefined) throw new Error("REDIS_URL is required");

const client = new Redis(url);
const dialect = new RedisDialect({ client, prefix: "billing:" });
```

The prefix defaults to `queue:` and namespaces every key, so several dialects can share one Redis. The dialect owns no client connection: `close()` only makes the dialect reject further calls, and the caller closes the client.

Each operation is one Lua script, so two consumers never claim the same message. The dialect targets a single Redis, not a cluster: keys are built inside the scripts. Exact `count()` costs a few O(log n) sorted-set counts plus a scan of expired leases not yet dequeued; `dequeue()` runs the same scan to requeue them.

## Effect

```ts
import { EffectQueue } from "@tailoredstack/queue/effect";
import { Effect } from "effect";

const queue = new EffectQueue(
  new Queue(new RedisDialect({ client: redis, prefix: "billing:" }), {
    jobs: { sendEmail: SendEmail },
  }),
);

const program = Effect.gen(function* () {
  yield* queue.enqueue(new SendEmail({ to: "ada@example.com" }), { delay: 5_000 });
  const message = yield* queue.dequeue();
  if (message) yield* queue.acknowledge(message.receipt);
  return yield* queue.count();
});
```

Every client method returns an `Effect`. Invalid options, unregistered job classes, payload validation, and dialect failures become a tagged `QueueError` carrying the operation and its cause. The `effect` peer is optional, so the core entry still has no Effect dependency.

## Node version

`SqliteDialect` is built on `node:sqlite`. The package declares `engines: node >= 24.16.0` and CI runs the workspace `.nvmrc` (Node 26).

**Node 22 users cannot use SQLite here.** `node:sqlite` is flag-gated before Node 22.13 (`--experimental-sqlite`) and still experimental after it, so it is outside the supported and tested range. Upgrade to Node 24+ for `SqliteDialect`; `MemoryDialect` and `RedisDialect` run on the same Node 24+ floor.

## Limits

- **Blocking:** `node:sqlite` is synchronous. A contended write can block the event loop for up to the 5 s `busy_timeout`, so run queue work in its own process if you also serve latency-sensitive requests there.
- **One host:** SQLite plus WAL is fine for many processes on one machine. For several hosts use `RedisDialect` or `SqsDialect` instead. WAL needs a local filesystem, not NFS/SMB.
- **Pass a file path.** `path: ":memory:"` gives every dialect instance its own private database, so two dialects never see each other's jobs.
- **Counts scan per queue name:** exact `count()` touches the rows of that name. Many queue names in one file are fine; a huge backlog makes counts proportionally costlier.
- **Schema:** the dialect owns its table (`table`, default `queue_messages`) and migrates it on construction (`CREATE TABLE/INDEX IF NOT EXISTS`). Point it at a file whose schema is managed elsewhere and it will still create its own table; it never touches tables it does not own.
- **Redis eviction and partial failures:** run Redis with `maxmemory-policy noeviction`. Evicting the id sequence can reuse ids, and evicting a message hash strands its reservation. Lua scripts are atomic against other clients but do not roll back partial writes if a command fails, so keep the prefix exclusive to this dialect.

## Running tests

```sh
pnpm --filter @tailoredstack/queue test
```

The SQLite shared suite and the cross-process SQLite lock test run against real files and need no Docker. The Redis test starts `redis:8-alpine` through testcontainers; set `REDIS_URL` to reuse an already-running server instead, for example `REDIS_URL=redis://127.0.0.1:6379 pnpm --filter @tailoredstack/queue test`. `SqsDialect` runs the same suite against a digest-pinned `floci/floci:2.4.0` through testcontainers, so its test file needs Docker. The suite adapts to a dialect's clock: `clockSource: 'caller'` dialects run on a synthetic timeline, while `clockSource: 'backend'` dialects use real, whole-second waits and the order case prints a named skip when `ordered` is false.

## License

ISC. See [LICENSE](../../LICENSE).
