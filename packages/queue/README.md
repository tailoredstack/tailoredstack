# @tailoredstack/queue

A typed job queue with pluggable dialects, modeled on Laravel queues. The backend is an injected object, so the core imports no driver and you pay only for the dialect you use.

## Install

```sh
pnpm add @tailoredstack/queue
```

## Dialects

| Dialect        | Storage                         | Ordering | Exact counts | Delay / lease caps |
| -------------- | ------------------------------- | -------- | ------------ | ------------------ |
| `MemoryDialect` | process memory                 | FIFO     | yes          | none               |
| `SqliteDialect` | a SQLite file (`node:sqlite`)  | FIFO     | yes          | none               |

```ts
import { MemoryDialect, Queue, SqliteDialect } from "@tailoredstack/queue";

new Queue(new MemoryDialect(), { jobs });
new SqliteDialect({ path: "./queue.db" }); // file, single host, uses node:sqlite, no server
```

## Node version

`SqliteDialect` is built on `node:sqlite`, so the package targets Node 24 and newer. **Node 22 users cannot use SQLite**: `node:sqlite` is not available there without experimental flags, and this package does not fall back to a native SQLite binding. On Node 22 use `MemoryDialect`, or upgrade to Node 24+ for the SQLite dialect. `MemoryDialect` and the core have no such requirement.

## Running tests

```sh
pnpm --filter @tailoredstack/queue test
```

The shared dialect suite and the cross-process SQLite lock test run against real files and need no Docker.

## License

ISC. See [LICENSE](../../LICENSE).
