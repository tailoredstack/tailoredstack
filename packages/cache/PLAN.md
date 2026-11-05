# @tailoredstack/cache: status

Status: v0 feature-complete. See README.md for the public API.

Milestones:
- 0 red suite cleanup: done
- 1 core (keys, Cache, EffectCache): done
- 2 drivers, shared contract (memory, sqlite, redis): done
- 3 exports and packaging (./effect, ./testing, optional peers): done
- 4 tags (Redis only, compile-time gated): done
- 5 docs: README done; this file trimmed to status

Open decisions for v1:
- Memory store tags (not in v0).
- Cache-level tests against live Redis beyond tags: only the store contract and tags run there.
- Tag rotation race: documented, not solved.
