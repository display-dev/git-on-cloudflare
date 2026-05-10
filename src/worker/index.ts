import { Hono } from "hono";
import { registerGitRoutes } from "./routes/git";
import { registerAdminRoutes } from "./routes/admin";
import { registerUiRoutes } from "./routes/ui";
import { registerAuthRoutes } from "./routes/auth";
import { requestServicesMiddleware, type AppBindings, type AppContext } from "./routes/hono";
import { renderUiDocumentResponse } from "./routes/uiResponse";
import { loadViewer } from "./auth/session";
import { json } from "./common";
import { handleRepoTaskQueue } from "./tasks/queue";

const app = new Hono<AppBindings>({ strict: false });
app.use("*", requestServicesMiddleware);
// Register Git protocol routes (info/refs, upload-pack, receive-pack)
registerGitRoutes(app);
// Register Admin routes
registerAdminRoutes(app);
// Register Auth routes BEFORE UI to avoid /:owner shadowing /auth
registerAuthRoutes(app);

app.get("/", async (c) => {
  const viewer = await loadViewer(c);
  return renderUiDocumentResponse(
    c.env,
    "home",
    {},
    { failureBody: "Failed to render page\n", viewer }
  );
});

// Register UI routes AFTER static/auth so that /:owner doesn't shadow them
registerUiRoutes(app);

async function renderNotFound(c: AppContext): Promise<Response> {
  const viewer = await loadViewer(c);
  return renderUiDocumentResponse(
    c.env,
    "404",
    {},
    {
      status: 404,
      failureBody: "Not found\n",
      failureStatus: 404,
      viewer,
    }
  );
}

app.notFound((c) => renderNotFound(c));

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
    return app.fetch(request, env, ctx);
  },
  async queue(batch: MessageBatch<unknown>, env: Env, ctx: ExecutionContext) {
    return await handleRepoTaskQueue(batch, env, ctx);
  },
};

export { RepoDurableObject } from "./do/repo/repoDO";
