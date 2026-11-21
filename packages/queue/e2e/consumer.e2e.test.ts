import { spawnSync } from "node:child_process";
import { cpSync, existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

// Packs the package through the release path (pnpm expands the catalog), installs the
// tarball into clean projects, and imports only the public entry points.
const pkgDir = fileURLToPath(new URL("..", import.meta.url));
const fixtures = fileURLToPath(new URL("./fixtures", import.meta.url));

let packDir: string;
let tarball: string;
const workDirs: string[] = [];

function run(file: string, args: string[], cwd: string): string {
  const result = spawnSync(file, args, { cwd, env: process.env, encoding: "utf8" });
  if (result.status !== 0) {
    throw new Error(
      `${file} ${args.join(" ")} exited ${result.status}\n${(result.stderr || result.stdout).slice(-2000)}`,
    );
  }
  return result.stdout;
}

function installConsumer(entry: string, dependencies: string[]): string {
  const work = mkdtempSync(join(tmpdir(), `queue-consumer-${entry}-`));
  workDirs.push(work);
  writeFileSync(join(work, "package.json"), JSON.stringify({ name: "consumer", private: true, type: "module" }));
  run(
    "npm",
    ["install", "--no-audit", "--no-fund", tarball, "typescript@^7", "@types/node@^26", ...dependencies],
    work,
  );
  cpSync(join(fixtures, `${entry}.ts`), join(work, `${entry}.ts`));
  cpSync(join(fixtures, "tsconfig.json"), join(work, "tsconfig.json"));
  run(join(work, "node_modules", ".bin", "tsc"), ["-p", "tsconfig.json"], work);
  run("node", [`${entry}.ts`], work);
  return work;
}

beforeAll(() => {
  run("pnpm", ["build"], pkgDir);
  packDir = mkdtempSync(join(tmpdir(), "queue-pack-"));
  const packed = JSON.parse(run("pnpm", ["pack", "--json", "--pack-destination", packDir], pkgDir)) as {
    filename: string;
  };
  if (!packed.filename) throw new Error("pnpm pack produced no tarball");
  tarball = packed.filename;
}, 600_000);

afterAll(() => {
  for (const work of workDirs) rmSync(work, { recursive: true, force: true });
  if (packDir) rmSync(packDir, { recursive: true, force: true });
});

describe("installed @tailoredstack/queue", () => {
  it("packs a manifest with expanded catalogs and an optional effect peer", () => {
    const manifest = JSON.parse(run("tar", ["-xzOf", tarball, "package/package.json"], pkgDir)) as {
      peerDependencies?: Record<string, string>;
      peerDependenciesMeta?: Record<string, { optional?: boolean }>;
    };
    expect(JSON.stringify(manifest)).not.toContain("catalog:");
    expect(manifest.peerDependencies?.effect).toMatch(/^\^?4\./);
    expect(manifest.peerDependenciesMeta?.effect?.optional).toBe(true);
  });

  it("typechecks and runs the ./effect entry from the packed tarball", () => {
    const work = installConsumer("consumer", ["effect@^4"]);
    expect(existsSync(join(work, "node_modules", "effect"))).toBe(true);
  });

  it("typechecks and runs the core entry with no effect installed", () => {
    const work = installConsumer("consumer-core", []);
    expect(existsSync(join(work, "node_modules", "effect"))).toBe(false);
  });
});
