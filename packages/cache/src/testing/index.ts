// Public compatibility suite for community drivers: `storeContract(name, make)` in your own test file.
// Requires `vitest` as the host test runner.
export { type Clock, type ContractOptions, makeClock, storeContract } from "./store-contract.ts";
