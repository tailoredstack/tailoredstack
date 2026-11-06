# Changelog

## 1.0.0 (2026-11-06)


### Features

* **queue:** add core Queue API over an injected driver ([7730481](https://github.com/tailoredstack/tailoredstack/commit/7730481f7edf786a1ad99a9201a2544090ca2498))
* **queue:** add sync driver that runs jobs inline ([abdc127](https://github.com/tailoredstack/tailoredstack/commit/abdc12777554fed329de1886866cf6c5bdaac574))
* **queue:** derive job names from register ([5809e65](https://github.com/tailoredstack/tailoredstack/commit/5809e651370ca1b8d2c8f9d31439e459d8beb837))
* **queue:** enforce job timeout and failOnTimeout in runJob ([538d40f](https://github.com/tailoredstack/tailoredstack/commit/538d40f23ec6d8eec6ef4804d2ff04c56307d34b))
* **queue:** fail jobs with unregistered names on the first attempt ([52060d6](https://github.com/tailoredstack/tailoredstack/commit/52060d6fd415de6d62f6e9e4aa1c777be209ca37))
* **queue:** failed-jobs store with retry, forget, flush, and prune ([24d406f](https://github.com/tailoredstack/tailoredstack/commit/24d406fd6f1777646a103b48fc2249ae86e56c27))
* **queue:** job options for tries, timeout, backoff, retryUntil ([6403bcc](https://github.com/tailoredstack/tailoredstack/commit/6403bcc61502798a82795db9c382351a87e04bf0))
* **queue:** register jobs by name ([b6082bc](https://github.com/tailoredstack/tailoredstack/commit/b6082bcf26ae97c2470fac4269ae98cefdb7c391))
* **queue:** reserved job API with delete, release, and fail ([fc1445d](https://github.com/tailoredstack/tailoredstack/commit/fc1445ddce5005730f587f13f8f37ac4c329c3e5))
* **queue:** typed job names reject unknown names at compile time ([e639049](https://github.com/tailoredstack/tailoredstack/commit/e639049fc692285f7a2d21005abe7b7df7137f23))


### Bug Fixes

* **queue:** check Date values returned from a custom toJSON ([3da1ac4](https://github.com/tailoredstack/tailoredstack/commit/3da1ac48cefc400abeb3ee3b21ab1cf4b88a163d))
* **queue:** decide unregistered from the job name, not the error ([c2ad535](https://github.com/tailoredstack/tailoredstack/commit/c2ad5354d730fef976d4f11697e6637fae095a1e))
* **queue:** keep a timed-out job leased until its reservation ends ([ff1b404](https://github.com/tailoredstack/tailoredstack/commit/ff1b404cc85ffc197561c9cc11f5ace39b674bd3))
* **queue:** reject invalid Date values in job data ([464bdc7](https://github.com/tailoredstack/tailoredstack/commit/464bdc73d9173119aca04b234a5eb76e7cd36afc))
* **queue:** reject non-finite numbers in job data ([75f2acf](https://github.com/tailoredstack/tailoredstack/commit/75f2acfa6950f0e33337d5b39945a58d952d50c3))
* **queue:** reject settle calls on an inline job ([1e62f8c](https://github.com/tailoredstack/tailoredstack/commit/1e62f8c78940560c37c557d7924e8fbf3e1b450d))
