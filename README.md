# tailoredstack

TypeScript backend tooling in the style of TanStack: small, typed packages with one consistent API over pluggable backends.

## Packages

| Package                                  | Status                | Description                                                                                                                                                     |
| ---------------------------------------- | --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`@tailoredstack/cache`](packages/cache) | 0.1.0 (first release) | One typed cache API over memory, SQLite, and Redis/Valkey stores. TanStack-style array keys, stale-while-revalidate, locks, and O(matches) prefix invalidation. |

## Install

Packages are published to GitHub Packages. Installs need a GitHub token with `read:packages`:

```ini
# .npmrc
@tailoredstack:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=${NPM_TOKEN}
```

```sh
pnpm add @tailoredstack/cache
```

## Development

This is a pnpm workspace. Requirements: Node 26 (see `.nvmrc`), pnpm 12 (see `packageManager`), and Docker for the Testcontainers-backed suites.

```sh
pnpm install
pnpm lint        # biome check
pnpm typecheck
pnpm build
pnpm test        # unit suites; starts Redis/Valkey containers
pnpm --filter @tailoredstack/cache test:e2e   # packs the package and runs it as a consumer
```

On Colima (macOS), set `TESTCONTAINERS_RYUK_DISABLED=true` for local runs.

## Contributing

- Commits and PR titles follow [Conventional Commits](https://www.conventionalcommits.org/). Releases are generated from them by release-please.
- Stack PRs on top of their base branch. Merge bottom-up.

## License

ISC. See [LICENSE](LICENSE).
