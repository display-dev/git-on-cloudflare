import { beforeAll, describe, expect, it } from "vitest";
import { env, exports as workerExports } from "cloudflare:workers";

import { ensureD1Migrations } from "./util/d1Setup";
import { setupRepoForTests } from "./util/repoSeed";

beforeAll(async () => {
  await ensureD1Migrations(env);
});

describe("Hono routing compatibility", () => {
  it("renders the root page and the shared 404 fallback", async () => {
    const root = await workerExports.default.fetch("https://example.com/");
    expect(root.status).toBe(200);
    expect(root.headers.get("X-Page-Renderer")).toBe("react-ssr");

    const missing = await workerExports.default.fetch("https://example.com/does/not/exist");
    expect(missing.status).toBe(404);
    expect(missing.headers.get("X-Page-Renderer")).toBe("react-ssr");
  });

  it("keeps auth precedence and trailing-slash route matching", async () => {
    const auth = await workerExports.default.fetch("https://example.com/auth/");
    expect(auth.status).toBe(200);
    expect(auth.headers.get("X-Page-Renderer")).toBe("react-ssr");

    const owner = "hono-routing-owner";
    await setupRepoForTests(env, owner, "placeholder");
    const ownerOverview = await workerExports.default.fetch(`https://example.com/${owner}/`);
    expect(ownerOverview.status).toBe(200);
    expect(ownerOverview.headers.get("X-Page-Renderer")).toBe("react-ssr");
    expect(await ownerOverview.text()).toContain(owner);
  });

  it("keeps unsupported methods on the rendered 404 fallback", async () => {
    const postOwner = await workerExports.default.fetch("https://example.com/hono-routing-owner", {
      method: "POST",
    });
    expect(postOwner.status).toBe(404);
    expect(postOwner.headers.get("X-Page-Renderer")).toBe("react-ssr");
  });
});
