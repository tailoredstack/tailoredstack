# Changelog

## 0.1.0 (2026-11-06)


### ⚠ BREAKING CHANGES

* **cache:** Cache.tags(), Cache.flush(), the tags option, TaggableStore and UnsupportedError were removed. Array keys cover the same grouping on every driver through invalidate().

### Features

* **cache:** accept tags option on set and getOrSet ([eba3495](https://github.com/tailoredstack/tailoredstack/commit/eba34956e698c7d02babb84f1e900ebabcfb3f1b))
* **cache:** add extendIf store primitive and contract tests ([89a4794](https://github.com/tailoredstack/tailoredstack/commit/89a4794b6866086dff45fe2e3bf1bbeaec1d2695))
* **cache:** add lock option to getOrSet ([211ce32](https://github.com/tailoredstack/tailoredstack/commit/211ce32e1aec1f40e71d82529c5d352b2379b561))
* **cache:** add redis prefix index sets ([bc19831](https://github.com/tailoredstack/tailoredstack/commit/bc1983164d846d881fadd11a363f53db8a59e97a))
* **cache:** add sqlite keysWithPrefix as a primary-key range scan ([8f4876a](https://github.com/tailoredstack/tailoredstack/commit/8f4876ad0fb1d74bf51a68167eee194e332ddd85))
* **cache:** implement extendIf in memory, sqlite and redis drivers ([4de2c5a](https://github.com/tailoredstack/tailoredstack/commit/4de2c5a41120886846990edfd0f10e4af008a686))
* **cache:** keep memory keys sorted for prefix scans ([d2179b0](https://github.com/tailoredstack/tailoredstack/commit/d2179b0eee6e064ea1915d3dc2c3c313a9f8fa0b))
* **cache:** one typed cache API over memory, sqlite and redis ([4a021f7](https://github.com/tailoredstack/tailoredstack/commit/4a021f7a3eda5f2580631305a4282d81b11e1700))
* **cache:** one typed cache API over memory, sqlite and redis ([912d297](https://github.com/tailoredstack/tailoredstack/commit/912d297869a6f022eec96d963184b73126d1e488))
* **cache:** prefix-indexed invalidation on every driver ([f244354](https://github.com/tailoredstack/tailoredstack/commit/f244354adf64409032ee3f71177d9050ad2dc779))
* **cache:** renew advisory locks while the callback runs ([c80e515](https://github.com/tailoredstack/tailoredstack/commit/c80e515b71625bc520b4e93da434930a250e3b9f))


### Bug Fixes

* **cache:** a release failure does not replace the callback outcome ([8e2ddd7](https://github.com/tailoredstack/tailoredstack/commit/8e2ddd71281b4cf4721d8f72a6fab490b62c8472))
* **cache:** close review findings in memory prefix scan, guards and waits ([1810442](https://github.com/tailoredstack/tailoredstack/commit/1810442a943a7c66b38e554eca9f2637204df48d))
* **cache:** declare engines.node &gt;=22.13.0 for the static node:sqlite import ([8757562](https://github.com/tailoredstack/tailoredstack/commit/8757562c7f04e3112eb704219bf25cbf8e1d95d6))
* **cache:** escape user values shaped like internal envelopes ([3db5287](https://github.com/tailoredstack/tailoredstack/commit/3db5287a580625746febaee9111faff508a3e891))
* **cache:** invalidate array keys through prefix queries, not a full scan ([47288df](https://github.com/tailoredstack/tailoredstack/commit/47288df0f8b1ae0f5b9cf64f6fd8b1e6fd611fd4))
* **cache:** keep ioredis types out of published declarations ([eed02d3](https://github.com/tailoredstack/tailoredstack/commit/eed02d3a0205ee35b59bf3e4117f21181f55a2fa))
* **cache:** key getOrSet single-flight on the effective ttl ([3c3d3ed](https://github.com/tailoredstack/tailoredstack/commit/3c3d3ed4630009c292d8cacfc35f7b5c82342844))
* **cache:** make sqlite increment atomic across processes ([2e882d7](https://github.com/tailoredstack/tailoredstack/commit/2e882d79a4cc94c4677f3b9be098e42a8febc564))
* **cache:** map a redis non-integer increment to TypeError ([551a8b1](https://github.com/tailoredstack/tailoredstack/commit/551a8b178d467cfc489beb85f21475f03e74e38a))
* **cache:** pass caller effect errors through the effect wrapper ([890b76a](https://github.com/tailoredstack/tailoredstack/commit/890b76aabccae2e1f6fea5332417d18412508750))
* **cache:** reject a redis increment that leaves the safe integer range ([5cb0903](https://github.com/tailoredstack/tailoredstack/commit/5cb090337cc995dc615288ba652c563cfa91b622))
* **cache:** reject flexible() fresh window longer than stale ([ad6d56a](https://github.com/tailoredstack/tailoredstack/commit/ad6d56af02ad467c5bbac3ca2ca67bf14edbff53))
* **cache:** reject non-JSON values and validate putMany before any write ([b206580](https://github.com/tailoredstack/tailoredstack/commit/b20658058e2a869f0fe0c93c6ac2bee62b5474ed))
* **cache:** reject non-safe-integer increment and decrement steps ([790e473](https://github.com/tailoredstack/tailoredstack/commit/790e47340609c37db6e1b3dc74c73cfce2b50067))
* **cache:** reject unserializable values and invalid increment steps ([d620d24](https://github.com/tailoredstack/tailoredstack/commit/d620d242e60986b28b85db32c07956c121be9a86))
* **cache:** reject values that cannot be serialized ([188149d](https://github.com/tailoredstack/tailoredstack/commit/188149d749e4cee2f01e19d453f33147223704ca))
* **cache:** report a failed lock release as an event, not a silent drop ([33aa9ec](https://github.com/tailoredstack/tailoredstack/commit/33aa9ecf0b7a1f52e0068806eed736b55f305bec))
* **cache:** resolve code review findings and prepare 0.1.0 release ([a2af7e0](https://github.com/tailoredstack/tailoredstack/commit/a2af7e0c0431724621c4768c2e80bab3172e8986))
* **cache:** round durations to ms and share flexible refreshes ([6395f5b](https://github.com/tailoredstack/tailoredstack/commit/6395f5b674f1bded6a4a84fe32e8f0b5c84f5a51))
* **cache:** serialize each value once and store the checked result ([702bba8](https://github.com/tailoredstack/tailoredstack/commit/702bba8fc8bf792bec6802a189269cbe9b3225a9))
* **cache:** share a getOrSet compute only when the stored result is the same ([c5c43fd](https://github.com/tailoredstack/tailoredstack/commit/c5c43fdaaa2bd6493033e76ef6ac0cab25b336d5))
* **cache:** single-flight getOrSet misses within one process ([91675b5](https://github.com/tailoredstack/tailoredstack/commit/91675b508175c88f239a68995ca7abe7db68e547))
* **cache:** throw when an increment leaves the safe integer range ([c236c65](https://github.com/tailoredstack/tailoredstack/commit/c236c65caa1541f18ec3950e20ee4cac30c3cfa3))
* **cache:** validate lock durations before taking the lock ([aa90f6d](https://github.com/tailoredstack/tailoredstack/commit/aa90f6d19ca9d8d8f3abd0f35245b61624dd0ef0))


### Code Refactoring

* **cache:** remove the explicit tags api ([7338561](https://github.com/tailoredstack/tailoredstack/commit/733856141498e7db934bdae162e863c3f755aac0))
