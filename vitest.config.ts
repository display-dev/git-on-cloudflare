// @ts-nocheck
import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { readFileSync, existsSync } from "node:fs";
import path from "path";
import { BASE_TEST_BINDINGS } from "./test/vitest.bindings";
import { defineConfig } from "vitest/config";

const AUTH_TEST_FILE = "test/auth.worker.test.ts";
const INLINE_DEPS = ["@noble/hashes", "pako"];
const isAuthSuite =
  process.env.npm_lifecycle_event === "test:auth" ||
  process.argv.some((arg) => arg.includes("auth.worker.test.ts"));

export default defineConfig({
  plugins: [
    cloudflareTest({
      main: "./src/worker/index.ts",
      wrangler: {
        configPath: "./wrangler.jsonc",
      },

      // Windows workaround: disable isolatedStorage to avoid EBUSY teardown failures
      // Avoid filesystem persistence to prevent SQLite CANTOPEN/EBUSY on Windows
      singleWorker: true,
      isolatedStorage: false,

      miniflare: {
        durableObjectsPersist: false,
        kvPersist: false,
        r2Persist: false,
        cachePersist: false,
        // Keep the Worker test runtime aligned with the deployed Worker.
        compatibilityDate: "2026-05-13",
        compatibilityFlags: ["nodejs_als"],
        bindings: {
          ...BASE_TEST_BINDINGS,
          PACK_INDEXER_FIXTURE: process.env.PACK_INDEXER_FIXTURE === "1" ? "1" : "",
        },
        serviceBindings: {
          // Service binding that runs in Node.js (not workerd) for reading
          // fixture files from disk. Tests call env.FIXTURE_READER.fetch()
          // with the file path as the URL pathname.
          async FIXTURE_READER(request: Request) {
            const url = new URL(request.url);
            const filePath = decodeURIComponent(url.pathname.slice(1));
            const resolved = path.resolve(__dirname, filePath);
            if (!existsSync(resolved)) {
              return new Response("not found", { status: 404 });
            }
            const data = readFileSync(resolved);
            return new Response(data, {
              headers: { "Content-Length": String(data.byteLength) },
            });
          },
        },
      },
    }),
  ],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  // Inline deps at the vite-node server level to avoid SSR optimizer resolution issues
  server: {
    deps: {
      inline: INLINE_DEPS,
    },
  },
  test: {
    include: isAuthSuite ? [AUTH_TEST_FILE] : ["test/**/*.worker.test.ts"],
    exclude: isAuthSuite ? [] : [AUTH_TEST_FILE],
  },
});
