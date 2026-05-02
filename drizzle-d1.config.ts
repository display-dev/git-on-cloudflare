import { defineConfig } from "drizzle-kit";

export default defineConfig({
  out: "./drizzle/d1",
  schema: "./src/worker/db/d1/schema/index.ts",
  dialect: "sqlite",
});
