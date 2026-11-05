# @tailoredstack/cache: status

Status: v0 feature-complete. See README.md for the public API.

Milestones:

- 0 red suite cleanup: done
- 1 core (keys, Cache, EffectCache): done
- 2 drivers, shared contract (memory, sqlite, redis): done
- 3 exports and packaging (./effect, ./testing, optional peers): done
- 4 tags (Redis only, compile-time gated): done, removed in 4b
- 5 docs: README done; this file trimmed to status
- 6 SQL stores (postgres, mysql, pglite): planned, see below

## Milestone 6: SQL stores (planned)

Drivers, following Kysely's official dialects: `pg`, `mysql2`, `@electric-sql/pglite`. Versions checked 2026-11-06: pg 8.23.1, mysql2 3.24.6, pglite 0.5.15.

Shape:

- `PostgresStore({ pool })` at `@tailoredstack/cache/postgres`
- `MysqlStore({ pool })` at `@tailoredstack/cache/mysql`
- `PgliteStore({ db })` at `@tailoredstack/cache/pglite`
- Clients injected, raw SQL, no Kysely dependency. Core never imports a driver.
- Postgres and PGlite share one implementation. MySQL differs in upsert syntax and column types.

Design:

- Expiry in JS with injected `now`, as `SqliteStore` does. Time-travel contract tests then run on SQL stores.
- `add`: `ON CONFLICT DO NOTHING` (Postgres), `INSERT IGNORE` (MySQL).
- `increment`: transaction with row lock, so the integer check holds.
- `forgetIf`: conditional DELETE.
- MySQL: InnoDB key limit (~768 chars) is too short for JSON array keys. Use a SHA-256 primary key plus the full key in a column, so `entries()` still works.
- Schema: `CREATE TABLE IF NOT EXISTS` on first use, table name option defaulting to `cache`.

Testing:

- PGlite first: real Postgres in WASM, no Docker, runs in CI without containers.
- Postgres and MySQL via `@testcontainers/postgresql` and `@testcontainers/mysql` (12.2.1), same pattern as Redis and Valkey.
- Each store runs `storeContract` and the e2e consumer check.

Order: PGlite, then Postgres, then MySQL.

Open decisions:

- Raw SQL with injected clients (recommended), or a Kysely dependency.
- MySQL keys: hashed primary key (recommended), or cap key length with `RangeError`.
- Schema: auto-create on first use (recommended), or user-run migrations.
