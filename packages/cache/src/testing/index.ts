// Public compatibility suite for community drivers: `storeContract(name, make)` in your own test file.
// Requires `vitest` as the host test runner.
export { storeContract, makeClock, type Clock, type ContractOptions } from "./store-contract.ts";
