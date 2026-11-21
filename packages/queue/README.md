# @tailoredstack/queue

A typed job queue with pluggable dialects, modeled on Laravel queues. The backend is an injected object, so the core imports no driver and you pay only for the dialect you use.

## Install

```sh
pnpm add @tailoredstack/queue
```

## Dialects

| Dialect         | Storage                       | Ordering          | Exact counts | Delay / lease caps |
| --------------- | ----------------------------- | ----------------- | ------------ | ------------------ |
| `MemoryDialect` | process memory                | earliest due first | yes         | none               |
| `SqliteDialect` | a SQLite file (`node:sqlite`) | earliest due first | yes         | none               |

"Earliest due first" means a job becomes claimable at `availableAt` (now, plus any delay) and the dialect serves the smallest `availableAt`; jobs whose delay expires earlier are served before jobs enqueued earlier. Ties keep insertion order.

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

## Node version

`SqliteDialect` is built on `node:sqlite`. The package declares `engines: node >= 24.16.0` and CI runs the workspace `.nvmrc` (Node 26).

**Node 22 users cannot use SQLite here.** `node:sqlite` is flag-gated before Node 22.13 (`--experimental-sqlite`) and still experimental after it, so it is outside the supported and tested range. Upgrade to Node 24+ for `SqliteDialect`; `MemoryDialect` runs on the same Node 24+ floor.

## Limits

- **Blocking:** `node:sqlite` is synchronous. A contended write can block the event loop for up to the 5 s `busy_timeout`, so run queue work in its own process if you also serve latency-sensitive requests there.
- **One host:** SQLite plus WAL is fine for many processes on one machine. For several hosts use a network broker (Redis/SQS dialects) instead. WAL needs a local filesystem, not NFS/SMB.
- **Pass a file path.** `path: ":memory:"` gives every dialect instance its own private database, so two dialects never see each other's jobs.
- **Counts scan per queue name:** exact `count()` touches the rows of that name. Many queue names in one file are fine; a huge backlog makes counts proportionally costlier.
- **Schema:** the dialect owns a `queue_messages` table in the file you pass and migrates it on construction (`CREATE TABLE/INDEX IF NOT EXISTS`). Do not point it at a database whose schema is managed elsewhere.

## Running tests

```sh
pnpm --filter @tailoredstack/queue test
```

The shared dialect suite and the cross-process SQLite lock test run against real files and need no Docker.

## License

ISC. See [LICENSE](../../LICENSE).
