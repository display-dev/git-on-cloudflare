import { defineConfig } from "drizzle-kit";

export default defineConfig({
  out: "./drizzle/repo-do",
  schema: "./src/worker/do/repo/db/schema.ts",
  dialect: "sqlite",
  driver: "durable-sqlite",
});
