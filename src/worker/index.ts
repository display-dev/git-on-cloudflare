import { Hono } from "hono";
import { registerGitRoutes } from "./routes/git";
import { registerAdminRoutes } from "./routes/admin";
import { registerUiRoutes } from "./routes/ui";
import { registerAuthRoutes } from "./routes/auth";
import type { AppBindings } from "./routes/hono";
import { renderUiDocumentResponse } from "./routes/uiResponse";
import { json } from "./common";
import { handleRepoMaintenanceQueue, type RepoMaintenanceQueueMessage } from "./maintenance/queue";

const app = new Hono<AppBindings>({ strict: false });
// Register Git protocol routes (info/refs, upload-pack, receive-pack)
registerGitRoutes(app);
// Register Admin routes
registerAdminRoutes(app);
// Register Auth routes BEFORE UI to avoid /:owner shadowing /auth
registerAuthRoutes(app);

app.get("/", async (c) => {
  return renderUiDocumentResponse(c.env, "home", {}, { failureBody: "Failed to render page\n" });
});

// Register UI routes AFTER static/auth so that /:owner doesn't shadow them
registerUiRoutes(app);

async function renderNotFound(env: Env): Promise<Response> {
  return renderUiDocumentResponse(
    env,
    "404",
    {},
    {
      status: 404,
      failureBody: "Not found\n",
      failureStatus: 404,
    }
  );
}

app.notFound((c) => renderNotFound(c.env));

function errorStatus(error: Error): number {
  if ("status" in error && typeof error.status === "number") {
    return error.status;
  }
  return 500;
}

app.onError((error) => {
  // The previous router converted uncaught handler failures into JSON. Most
  // routes catch expected failures themselves, but keep this last-resort shape
  // stable for truly unexpected errors.
  const status = errorStatus(error);
  return json({ error: error.message || "Internal Server Error" }, status, {
    "Content-Type": "application/json; charset=utf-8",
  });
});

export default {
  fetch(request: Request, env: Env, ctx: ExecutionContext) {
    // Hono maps HEAD to GET before routing, but this service historically
    // treated HEAD as an unsupported method that reaches the rendered 404.
    if (request.method === "HEAD") {
      return renderNotFound(env);
    }
    return app.fetch(request, env, ctx);
  },
  async queue(batch: MessageBatch<RepoMaintenanceQueueMessage>, env: Env, ctx: ExecutionContext) {
    return await handleRepoMaintenanceQueue(batch, env, ctx);
  },
};

export { RepoDurableObject } from "./do/repo/repoDO";
export { AuthDurableObject } from "./do/auth/authDO";
