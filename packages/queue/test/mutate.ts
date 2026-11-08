// Applies one text mutant at a time to a file under src/, runs the full test suite, and records which
// contract or unit cases fail. Every source file is restored in a finally block. Writes test/mutation-matrix.md.
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

interface Mutant {
  id: string;
  file: string;
  rule: string;
  find: string;
  replace: string;
  cases: string[];
}

type MutantResult = "SURVIVED" | "KILLED" | "KILLED (other case)";

interface MutantRow extends Mutant {
  killers: string[];
  result: MutantResult;
}

interface VitestTest {
  title: string;
  fullName: string;
  status: string;
}

interface VitestFile {
  status: string;
  assertionResults: VitestTest[];
}

interface VitestReport {
  testResults: VitestFile[];
}

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const matrixPath = join(root, "test", "mutation-matrix.md");

const MUTANTS: Mutant[] = [
  // memory driver: rules R1-R12 on the stored state
  {
    id: "M1",
    file: "src/drivers/memory.ts",
    rule: "R2: availableAt == now is ready (reserve skips it)",
    find: "|| entry.availableAt > now) {",
    replace: "|| entry.availableAt >= now) {",
    cases: ["P1"],
  },
  {
    id: "M2",
    file: "src/drivers/memory.ts",
    rule: "R2/R12: availableAt == now counted as delayed",
    find: "} else if (entry.availableAt > now) {",
    replace: "} else if (entry.availableAt >= now) {",
    cases: ["Z4"],
  },
  {
    id: "M3",
    file: "src/drivers/memory.ts",
    rule: "R3: lease still live at now == leaseUntil",
    find: "entry.leaseUntil !== null && entry.leaseUntil > now",
    replace: "entry.leaseUntil !== null && entry.leaseUntil >= now",
    cases: ["S9", "Z3"],
  },
  {
    id: "M4",
    file: "src/drivers/memory.ts",
    rule: "R1: equal availableAt picks the later push",
    find: "entry.availableAt < best.availableAt) {",
    replace: "entry.availableAt <= best.availableAt) {",
    cases: ["S5"],
  },
  {
    id: "M5",
    file: "src/drivers/memory.ts",
    rule: "R1: ignores availableAt, takes the first push",
    find: "if (best === undefined || entry.availableAt < best.availableAt) {",
    replace: "if (best === undefined) {",
    cases: ["S4"],
  },
  {
    id: "M6",
    file: "src/drivers/memory.ts",
    rule: "R10: reserve does not increment attempts",
    find: "best.attempts += 1;",
    replace: "",
    cases: ["S3"],
  },
  {
    id: "M7",
    file: "src/drivers/memory.ts",
    rule: "R10: release resets attempts",
    find: "entry.availableAt = availableAt;",
    replace: "entry.availableAt = availableAt;\n      entry.attempts = 0;",
    cases: ["L3"],
  },
  {
    id: "M8",
    file: "src/drivers/memory.ts",
    rule: "R8: token constant across reservations",
    // biome-ignore lint/suspicious/noTemplateCurlyInString: find/replace text is source code, not a template
    find: "best.token = `t${tokenSequence}`;",
    replace: 'best.token = "t";',
    cases: ["D4", "L5"],
  },
  {
    id: "M9",
    file: "src/drivers/memory.ts",
    rule: "R6: expired lease still accepts delete",
    find: "return entry !== undefined && isLeased(entry, now) ? entry : undefined;",
    replace: "return entry;",
    cases: ["D3", "I2"],
  },
  {
    id: "M10",
    file: "src/drivers/memory.ts",
    rule: "R5: release keeps the lease, so the message stays reserved",
    find: "entry.leaseUntil = null;",
    replace: "",
    cases: ["L2"],
  },
  {
    id: "M11",
    file: "src/drivers/memory.ts",
    rule: "R5: release keeps the old availableAt",
    find: "entry.availableAt = availableAt;",
    replace: "",
    cases: ["L1"],
  },
  {
    id: "M12",
    file: "src/drivers/memory.ts",
    rule: "R5: release moves the message to the back of the push order",
    find: "entry.availableAt = availableAt;",
    replace:
      "entry.availableAt = availableAt;\n      entries = [...entries.filter((candidate) => candidate !== entry), entry];",
    cases: ["L4"],
  },
  {
    id: "M13",
    file: "src/drivers/memory.ts",
    rule: "R6: release accepts an expired token",
    find: "const entry = current(name, token, now);\n      if (entry === undefined) {\n        return false;\n      }\n      entry.availableAt = availableAt;",
    replace:
      "const entry = entries.find((candidate) => candidate.name === name && candidate.token === token);\n      if (entry === undefined) {\n        return false;\n      }\n      entry.availableAt = availableAt;",
    cases: ["L5"],
  },
  {
    id: "M14",
    file: "src/drivers/memory.ts",
    rule: "R2: push stores now instead of availableAt",
    find: "availableAt: at.availableAt,",
    replace: "availableAt: at.now,",
    cases: ["P2"],
  },
  {
    id: "M15",
    file: "src/drivers/memory.ts",
    rule: "R7: ids are not unique",
    // biome-ignore lint/suspicious/noTemplateCurlyInString: find/replace text is source code, not a template
    find: "const id = `m${idSequence}`;",
    replace: 'const id = "m";',
    cases: ["P10", "P11"],
  },
  {
    id: "M16",
    file: "src/drivers/memory.ts",
    rule: "R12/Z5: size counts other names",
    find: "if (entry.name !== name) {",
    replace: "if (false) {",
    cases: ["Z5"],
  },
  {
    id: "M17",
    file: "src/drivers/memory.ts",
    rule: "R12: reserved ignores lease time",
    find: "if (isLeased(entry, now)) {",
    replace: "if (entry.leaseUntil !== null) {",
    cases: ["Z3"],
  },
  {
    id: "M18",
    file: "src/drivers/memory.ts",
    rule: "R6: reserve hands out a leased message again",
    find: "entry.name !== name || isLeased(entry, now) ||",
    replace: "entry.name !== name ||",
    cases: ["S6", "B8"],
  },
  // wrapper: check order, close, token binding, clamp, payload and attempts exposure
  {
    id: "Q1",
    file: "src/queue.ts",
    rule: "R11/C2: calls after close are not rejected",
    find: "if (closing) {",
    replace: "if (false) {",
    cases: ["C2"],
  },
  {
    id: "Q2",
    file: "src/queue.ts",
    rule: "C3: close does not wait for in-flight calls",
    find: "if (inFlight > 0) {",
    replace: "if (false) {",
    cases: ["C3"],
  },
  {
    id: "Q3",
    file: "src/queue.ts",
    rule: "C3: close never resolves after in-flight calls drain",
    find: "drained?.resolve();",
    replace: "",
    cases: ["C3"],
  },
  {
    id: "Q4",
    file: "src/queue.ts",
    rule: "C1: close is not memoized, so the driver closes twice",
    find: "closed ??= (async () => {",
    replace: "closed = (async () => {",
    cases: ["C1"],
  },
  {
    id: "Q5",
    file: "src/queue.ts",
    rule: "R8: delete passes the message id instead of the token",
    find: "return driver.delete(name, token, now);",
    replace: "return driver.delete(name, lease.id, now);",
    cases: ["D1", "D2"],
  },
  {
    id: "Q6",
    file: "src/queue.ts",
    rule: "R9: release does not clamp a past availableAt",
    find: "const clamped = clampAvailableAt(availableAt, now);",
    replace: "const clamped = availableAt;",
    cases: ["L2"],
  },
  {
    id: "Q7",
    file: "src/queue.ts",
    rule: "R9: push does not clamp a past availableAt",
    find: "const availableAt = clampAvailableAt(at.availableAt, at.now);",
    replace: "const availableAt = at.availableAt;",
    cases: ["P3"],
  },
  {
    id: "Q8",
    file: "src/queue.ts",
    rule: "S7/S8: reserve skips the lease window check",
    find: "validateLeaseWindow(now, leaseUntil);",
    replace: "",
    cases: ["S7", "S8"],
  },
  {
    id: "Q9",
    file: "src/queue.ts",
    rule: "B2/B3: reserve skips the leaseUntil time check",
    find: 'validateTime("leaseUntil", leaseUntil);',
    replace: "",
    cases: ["B2"],
  },
  {
    id: "Q10",
    file: "src/queue.ts",
    rule: "B1: size skips the name check",
    find: 'validateName(name);\n        validateTime("now", now);\n        return driver.size(name, now);',
    replace: 'validateTime("now", now);\n        return driver.size(name, now);',
    cases: ["B1"],
  },
  {
    id: "Q11",
    file: "src/queue.ts",
    rule: "R13/B5: Reserved payload is not the stored payload",
    find: "payload: lease.payload,",
    replace: 'payload: "",',
    cases: ["B5"],
  },
  {
    id: "Q12",
    file: "src/queue.ts",
    rule: "R10: Reserved attempts is not the driver value",
    find: "attempts: lease.attempts,",
    replace: "attempts: 0,",
    cases: ["S3"],
  },
  {
    id: "Q13",
    file: "src/queue.ts",
    rule: "B2: delete skips the now check",
    find: 'validateTime("now", now);\n          return driver.delete(name, token, now);',
    replace: "return driver.delete(name, token, now);",
    cases: ["B2"],
  },
  {
    id: "Q14",
    file: "src/queue.ts",
    rule: "L6: release skips the availableAt check",
    find: 'validateTime("availableAt", availableAt);',
    replace: "",
    cases: ["L6"],
  },
  // validation rules
  {
    id: "V1",
    file: "src/validate.ts",
    rule: "P7: name accepts 81 characters",
    find: "[A-Za-z0-9_-]{1,80}$/;",
    replace: "[A-Za-z0-9_-]{1,81}$/;",
    cases: ["P7", "B1"],
  },
  {
    id: "V2",
    file: "src/validate.ts",
    rule: "P7: name accepts the empty string",
    find: "[A-Za-z0-9_-]{1,80}$/;",
    replace: "[A-Za-z0-9_-]{0,80}$/;",
    cases: ["P7", "B1"],
  },
  {
    id: "V3",
    file: "src/validate.ts",
    rule: "P6/B4: payload at exactly 262144 bytes is rejected",
    find: "> MAX_PAYLOAD_BYTES",
    replace: ">= MAX_PAYLOAD_BYTES",
    cases: ["P6", "B4"],
  },
  {
    id: "V4",
    file: "src/validate.ts",
    rule: "P6: payload measured in characters, not UTF-8 bytes",
    find: "encoder.encode(payload).byteLength",
    replace: "payload.length",
    cases: ["P6", "B4"],
  },
  {
    id: "V5",
    file: "src/validate.ts",
    rule: "P5: lone surrogate accepted",
    find: "if (LONE_SURROGATE.test(payload)) {",
    replace: "if (false) {",
    cases: ["P5"],
  },
  {
    id: "V6",
    file: "src/validate.ts",
    rule: "P8: negative time accepted",
    find: "!Number.isInteger(value) || value < 0",
    replace: "!Number.isInteger(value)",
    cases: ["P8", "B2"],
  },
  {
    id: "V7",
    file: "src/validate.ts",
    rule: "P8: fractional and infinite time accepted",
    find: "!Number.isInteger(value) || value < 0",
    replace: "Number.isNaN(value) || value < 0",
    cases: ["P8", "B2"],
  },
  {
    id: "V8",
    file: "src/validate.ts",
    rule: "S7: leaseUntil == now accepted",
    find: "if (leaseUntil <= now) {",
    replace: "if (leaseUntil < now) {",
    cases: ["S7", "B3"],
  },
  {
    id: "V9",
    file: "src/validate.ts",
    rule: "S8/B10: a 12-hour lease window rejected",
    find: "leaseUntil - now > MAX_LEASE_MS",
    replace: "leaseUntil - now >= MAX_LEASE_MS",
    cases: ["S8", "B10"],
  },
  {
    id: "V10",
    file: "src/validate.ts",
    rule: "S8/B10: lease window not limited",
    find: "if (leaseUntil - now > MAX_LEASE_MS) {",
    replace: "if (false) {",
    cases: ["S8", "B10"],
  },
  {
    id: "V11",
    file: "src/validate.ts",
    rule: "L7: release delay not limited",
    find: "if (availableAt - now > MAX_LEASE_MS) {",
    replace: "if (false) {",
    cases: ["L7"],
  },
  {
    id: "V12",
    file: "src/validate.ts",
    rule: "P9: push delay not limited",
    find: "if (availableAt - now > maxPushDelayMs) {",
    replace: "if (false) {",
    cases: ["P9"],
  },
  {
    id: "V13",
    file: "src/validate.ts",
    rule: "P9: push delay equal to maxPushDelayMs rejected",
    find: "availableAt - now > maxPushDelayMs",
    replace: "availableAt - now >= maxPushDelayMs",
    cases: ["P9"],
  },
  {
    id: "V14",
    file: "src/validate.ts",
    rule: "R9/P3: clamp keeps a past availableAt",
    find: "return Math.max(availableAt, now);",
    replace: "return availableAt;",
    cases: ["P3", "L2"],
  },
  {
    id: "V15",
    file: "src/validate.ts",
    rule: "R9: clamp moves availableAt to the future",
    find: "return Math.max(availableAt, now);",
    replace: "return Math.min(availableAt, now);",
    cases: ["P3"],
  },
];

function runSuite(outFile: string): number | null {
  const result = spawnSync("pnpm", ["exec", "vitest", "run", "--reporter=json", `--outputFile=${outFile}`], {
    cwd: root,
    encoding: "utf8",
  });
  if (result.error) {
    throw result.error;
  }
  return result.status;
}

function failingCases(outFile: string): string[] {
  const report = JSON.parse(readFileSync(outFile, "utf8")) as VitestReport;
  const cases = new Set<string>();
  for (const file of report.testResults) {
    if (file.assertionResults.length === 0 && file.status === "failed") {
      cases.add("<suite-error>");
    }
    for (const test of file.assertionResults) {
      if (test.status !== "failed") {
        continue;
      }
      const head = test.title.split(":")[0];
      for (const id of head.match(/\b[A-Z]\d{1,2}\b/g) ?? [`untagged: ${test.fullName}`]) {
        cases.add(id);
      }
    }
  }
  return [...cases].sort();
}

function applyMutant(mutant: Mutant, original: string): string {
  const occurrences = original.split(mutant.find).length - 1;
  if (occurrences !== 1) {
    throw new Error(`${mutant.id}: expected one match in ${mutant.file}, found ${occurrences}`);
  }
  return original.replace(mutant.find, mutant.replace);
}

const scratch = mkdtempSync(join(tmpdir(), "queue-mutants-"));
const rows: MutantRow[] = [];
let failed = false;
const only = process.argv[2];

try {
  const baselineStatus = runSuite(join(scratch, "baseline.json"));
  if (baselineStatus !== 0) {
    throw new Error(`baseline suite must be green before mutating (exit ${baselineStatus})`);
  }

  for (const mutant of MUTANTS.filter((m) => only === undefined || m.id === only)) {
    const path = join(root, mutant.file);
    const original = readFileSync(path, "utf8");
    const outFile = join(scratch, `${mutant.id}.json`);
    let status: number | null;
    try {
      writeFileSync(path, applyMutant(mutant, original));
      status = runSuite(outFile);
    } finally {
      writeFileSync(path, original);
    }
    const killers = status === 0 ? [] : failingCases(outFile);
    const expectedHit = mutant.cases.some((id) => killers.includes(id));
    const result: MutantResult = status === 0 ? "SURVIVED" : expectedHit ? "KILLED" : "KILLED (other case)";
    if (result !== "KILLED") {
      failed = true;
    }
    rows.push({ ...mutant, killers, result });
    console.log(`${mutant.id} ${result} expected=${mutant.cases.join(",")} killers=${killers.join(",") || "-"}`);
  }
} finally {
  rmSync(scratch, { recursive: true, force: true });
}

const table = [
  "# Mutation matrix",
  "",
  "Generated by `pnpm run mutate` (`test/mutate.ts`). Each row applies one mutant to one source file, runs the full suite, and restores the file.",
  "A mutant is KILLED when a listed case fails. Killers are the case IDs that actually failed.",
  "",
  "| ID | File | Rule or check | Expected killers | Actual killers | Result |",
  "| --- | --- | --- | --- | --- | --- |",
  ...rows.map(
    (row) =>
      `| ${row.id} | ${row.file} | ${row.rule} | ${row.cases.join(", ")} | ${row.killers.join(", ") || "-"} | ${row.result} |`,
  ),
  "",
].join("\n");
writeFileSync(matrixPath, table);
const format = spawnSync("pnpm", ["exec", "biome", "format", "--write", matrixPath], { cwd: root, encoding: "utf8" });
if (format.status !== 0) {
  throw new Error(`biome format failed for ${matrixPath}: ${format.stderr}`);
}

process.exitCode = failed ? 1 : 0;
