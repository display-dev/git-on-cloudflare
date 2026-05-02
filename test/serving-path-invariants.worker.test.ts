import { describe, expect, it } from "vitest";

// Read source files at vite-node import time and grep them for forbidden
// imports / symbols. The serving-path invariant says Git, admin, and UI
// repo-serving handlers must not depend on the new ownership-resolver,
// the cache-policy discriminator, or the PAT verifier until the
// visibility-aware ACL hookup is wired through the resolver. The check
// runs inside workerd via `import.meta.glob`, so it stays portable
// without filesystem APIs.

const GIT_SOURCES = import.meta.glob("../src/worker/routes/git.ts", {
  query: "?raw",
  import: "default",
  eager: true,
}) as Record<string, string>;

const ADMIN_SOURCES = import.meta.glob("../src/worker/routes/admin.ts", {
  query: "?raw",
  import: "default",
  eager: true,
}) as Record<string, string>;

const UI_SOURCES = import.meta.glob("../src/worker/routes/ui/**/*.ts", {
  query: "?raw",
  import: "default",
  eager: true,
}) as Record<string, string>;

const FORBIDDEN_IMPORT_PATTERNS = [
  /from\s+["']@\/worker\/repositories\/route["']/,
  /from\s+["']@\/worker\/repositories["']/,
  /from\s+["']@\/worker\/cache\/policy["']/,
];
const FORBIDDEN_SYMBOL_PATTERN = /\bverifyPat\b/;

function fileBundles(): Array<{ path: string; source: string }> {
  return Object.entries({ ...GIT_SOURCES, ...ADMIN_SOURCES, ...UI_SOURCES }).map(
    ([path, source]) => ({
      path,
      source,
    })
  );
}

describe("serving-path invariants", () => {
  it("Git, admin, and UI repo-serving routes do not import the resolver/cache-policy modules", () => {
    const offenses: string[] = [];
    for (const { path, source } of fileBundles()) {
      for (const pattern of FORBIDDEN_IMPORT_PATTERNS) {
        if (pattern.test(source)) offenses.push(`${path} matches ${pattern}`);
      }
    }
    expect(offenses).toEqual([]);
  });

  it("Git, admin, and UI repo-serving routes do not call the PAT verifier", () => {
    const offenses: string[] = [];
    for (const { path, source } of fileBundles()) {
      if (FORBIDDEN_SYMBOL_PATTERN.test(source)) {
        offenses.push(`${path} mentions verifyPat`);
      }
    }
    expect(offenses).toEqual([]);
  });

  it("loaded the expected files (sanity check)", () => {
    const paths = fileBundles().map((file) => file.path);
    expect(paths.some((path) => path.endsWith("/git.ts"))).toBe(true);
    expect(paths.some((path) => path.endsWith("/admin.ts"))).toBe(true);
    expect(paths.some((path) => path.includes("/routes/ui/"))).toBe(true);
  });
});
