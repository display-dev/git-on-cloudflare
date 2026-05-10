import { beforeAll, describe, expect, it } from "vitest";
import { env } from "cloudflare:workers";

import {
  PAT_LAST_USED_READ_THROTTLE_MS,
  generatePatPlaintext,
  hashPatPlaintext,
  shouldTouchPatLastUsedAt,
} from "@/worker/auth/pat";
import { newPrefixedId } from "@/worker/common";
import { createDb } from "@/worker/db/d1/client";
import { findPatByPrefix, insertPatWithGrants, updatePatLastUsedAt } from "@/worker/db/d1/dal";

import { ensureD1Migrations } from "./util/d1Setup";
import { seedRepo } from "./util/repoSeed";

beforeAll(async () => {
  await ensureD1Migrations(env);
});

describe("shouldTouchPatLastUsedAt", () => {
  it("write op always returns true regardless of lastUsedAt", () => {
    expect(shouldTouchPatLastUsedAt(null, "write")).toBe(true);
    expect(shouldTouchPatLastUsedAt(Date.now(), "write")).toBe(true);
  });

  it("read op returns true when lastUsedAt is null", () => {
    expect(shouldTouchPatLastUsedAt(null, "read")).toBe(true);
  });

  it("read op returns false when lastUsedAt is fresh", () => {
    const now = Date.now();
    expect(shouldTouchPatLastUsedAt(now - 1000, "read", now)).toBe(false);
    expect(shouldTouchPatLastUsedAt(now - PAT_LAST_USED_READ_THROTTLE_MS + 1, "read", now)).toBe(
      false
    );
  });

  it("read op returns true when lastUsedAt is older than the throttle window", () => {
    const now = Date.now();
    expect(shouldTouchPatLastUsedAt(now - PAT_LAST_USED_READ_THROTTLE_MS, "read", now)).toBe(true);
    expect(shouldTouchPatLastUsedAt(now - PAT_LAST_USED_READ_THROTTLE_MS - 1, "read", now)).toBe(
      true
    );
  });
});

describe("updatePatLastUsedAt", () => {
  it("writes the supplied timestamp to the row", async () => {
    const seed = await seedRepo(env, {
      namespaceSlug: `pat-touch-${Math.random().toString(36).slice(2, 8)}`,
      repoSlug: "site",
    });
    const db = createDb(env.DB);
    const generated = generatePatPlaintext();
    const patId = newPrefixedId("pat");
    await insertPatWithGrants(db, {
      pat: {
        id: patId,
        userId: seed.userId,
        name: "ci",
        prefix: generated.publicPrefix,
        hash: await hashPatPlaintext(generated.plaintext),
        createdAt: Date.now(),
        expiresAt: null,
        revokedAt: null,
        lastUsedAt: null,
      },
      namespaceGrants: [],
      repoGrants: [{ patId, repoId: seed.repositoryId, level: "push" }],
    });

    const before = await findPatByPrefix(db, generated.publicPrefix);
    expect(before?.lastUsedAt).toBeNull();

    const stamp = 1_700_000_000_000;
    await updatePatLastUsedAt(db, patId, stamp);

    const after = await findPatByPrefix(db, generated.publicPrefix);
    expect(after?.lastUsedAt).toBe(stamp);
  });
});
