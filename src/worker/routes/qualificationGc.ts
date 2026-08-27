import { z } from "zod";
import { getRepoStub, json } from "@/worker/common";
import { GC_FAULTS } from "@/worker/git/maintenance/gcOperation";
import { gcOperationStatus } from "@/worker/git/maintenance/gcStatus";
import { packIndexKey, packRefsKey } from "@/worker/keys";
import { readPublishedRepositoryGenerationState } from "@/worker/git/generation/publish";
import type { RepositoryRoute } from "@/worker/repositories/route";
import type { AppContext, AppRouter } from "./hono";

const admissionSchema = z
  .object({
    schemaVersion: z.literal(1),
    operationId: z.string().regex(/^[A-Za-z0-9_-]{1,100}$/),
    faults: z
      .array(z.enum(GC_FAULTS))
      .max(GC_FAULTS.length)
      .refine((value) => new Set(value).size === value.length),
    holdReader: z.boolean(),
    deadlineAt: z.number().int().positive(),
  })
  .strict();

type Guards = {
  authorize(c: AppContext): Promise<Response | null>;
  resolveTarget(c: AppContext): Promise<RepositoryRoute | null>;
};

export function registerQualificationGcRoutes(router: AppRouter, guards: Guards): void {
  router.get("/_internal/qualification/:owner/:repo/gc-source", async (c) => {
    const denied = await guards.authorize(c);
    if (denied) return denied;
    const route = await guards.resolveTarget(c);
    if (!route) return new Response("Not found\n", { status: 404 });
    const stub = getRepoStub(c.env, route.doName);
    const packs = await c.var.limiter.run("do:qualification-gc-source", () =>
      stub.getActivePackCatalog()
    );
    const refs = await c.var.limiter.run("do:qualification-gc-refs", () => stub.listRefs());
    const published = await readPublishedRepositoryGenerationState({
      env: c.env,
      doId: stub.id.toString(),
      limiter: c.var.limiter,
      countSubrequest: () => {},
    });
    if (
      !published ||
      packs.length !== published.activePackKeys.size ||
      !packs.every((pack) => published.activePackKeys.has(pack.packKey))
    )
      return json({ status: "publication-pending" }, 409, { "Cache-Control": "no-store" });
    return json(
      {
        schemaVersion: 1,
        generation: published.generation,
        refs,
        packs: packs.map((pack) => ({
          packBytes: pack.packBytes,
          idxBytes: pack.idxBytes,
          objectCount: pack.objectCount,
        })),
      },
      200,
      { "Cache-Control": "no-store" }
    );
  });
  router.post("/_internal/qualification/:owner/:repo/gc", async (c) => {
    const denied = await guards.authorize(c);
    if (denied) return denied;
    const size = Number(c.req.header("Content-Length"));
    if (!Number.isSafeInteger(size) || size < 1 || size > 4096)
      return json({ status: "rejected" }, 400);
    const parsed = admissionSchema.safeParse(await c.req.json().catch(() => null));
    if (!parsed.success) return json({ status: "rejected" }, 400);
    const route = await guards.resolveTarget(c);
    if (!route) return new Response("Not found\n", { status: 404 });
    const stub = getRepoStub(c.env, route.doName);
    const result = await c.var.limiter.run(
      "do:qualification-gc-admit",
      async () =>
        await stub.registerQualificationGc(route.doName, parsed.data.operationId, {
          faults: parsed.data.faults,
          holdReader: parsed.data.holdReader,
          deadlineAt: parsed.data.deadlineAt,
        })
    );
    // Admission's alarm is sufficient for execution, including a lost HTTP
    // response. This control deliberately has no independent retry mechanism.
    return json(
      result.status === "ready" ? gcOperationStatus(result.operation) : { status: result.status },
      result.status === "ready" ? 202 : 409,
      { "Cache-Control": "no-store" }
    );
  });

  router.get("/_internal/qualification/:owner/:repo/gc/:operationId", async (c) => {
    const denied = await guards.authorize(c);
    if (denied) return denied;
    const route = await guards.resolveTarget(c);
    if (!route) return new Response("Not found\n", { status: 404 });
    const stub = getRepoStub(c.env, route.doName);
    const operation = await c.var.limiter.run("do:qualification-gc-status", () =>
      stub.getGcOperation()
    );
    if (!operation || operation.id !== c.req.param("operationId"))
      return new Response("Not found\n", { status: 404 });
    const published = await readPublishedRepositoryGenerationState({
      env: c.env,
      doId: stub.id.toString(),
      limiter: c.var.limiter,
      countSubrequest: () => {},
    });
    const sourceObjects =
      operation.snapshot?.sourcePacks.flatMap((pack) => [
        pack.packKey,
        packIndexKey(pack.packKey),
        packRefsKey(pack.packKey),
      ]) ?? [];
    let sourceObjectsPresent = 0;
    let sourceBytesPresent = 0;
    for (const key of sourceObjects) {
      const head = await c.var.limiter.run("r2:qualification-gc-source-head", () =>
        c.env.REPO_BUCKET.head(key)
      );
      if (head) {
        sourceObjectsPresent++;
        sourceBytesPresent += head.size;
      }
    }
    return json(
      {
        ...gcOperationStatus(operation),
        r2: {
          publishedGeneration: published?.generation ?? null,
          targetPublished: Boolean(
            operation.commit?.targetPackKey &&
            published?.activePackKeys.has(operation.commit.targetPackKey)
          ),
          sourceObjectsPresent,
          sourceBytesPresent,
        },
      },
      200,
      { "Cache-Control": "no-store" }
    );
  });

  router.post("/_internal/qualification/:owner/:repo/gc/:operationId/release-reader", async (c) => {
    const denied = await guards.authorize(c);
    if (denied) return denied;
    const route = await guards.resolveTarget(c);
    if (!route) return new Response("Not found\n", { status: 404 });
    const released = await c.var.limiter.run("do:qualification-gc-release-reader", () =>
      getRepoStub(c.env, route.doName).releaseGcReader(c.req.param("operationId"))
    );
    return json({ schemaVersion: 1, released }, released ? 200 : 409, {
      "Cache-Control": "no-store",
    });
  });

  const artifact = async (c: AppContext) => {
    const denied = await guards.authorize(c);
    if (denied) return denied;
    const role = c.req.param("role") ?? "";
    if (!["pack", "index", "references"].includes(role))
      return new Response("Not found\n", { status: 404 });
    const route = await guards.resolveTarget(c);
    if (!route) return new Response("Not found\n", { status: 404 });
    const stub = getRepoStub(c.env, route.doName);
    const lease = await c.var.limiter.run(
      "do:qualification-artifact-reader",
      async () => await stub.beginRepositoryRead()
    );
    if (!lease.ok) return new Response("Repository unavailable\n", { status: 409 });
    let released = false;
    const release = async () => {
      if (released) return;
      released = true;
      clearInterval(heartbeat);
      await c.var.limiter.run("do:qualification-artifact-reader-release", () =>
        stub.finishRepositoryRead(lease.token)
      );
    };
    let heartbeatFailure: Error | undefined;
    const heartbeat = setInterval(() => {
      void c.var.limiter
        .run("do:qualification-artifact-reader-renew", () => stub.renewRepositoryRead(lease.token))
        .then((renewed) => {
          if (!renewed) heartbeatFailure = new Error("Artifact reader lease expired");
        })
        .catch(() => {
          heartbeatFailure = new Error("Artifact reader lease unavailable");
        });
    }, 30_000);
    try {
      let target: string | undefined;
      if (c.req.param("operationId")) {
        const operation = await c.var.limiter.run("do:qualification-artifact-operation", () =>
          stub.getGcOperation()
        );
        if (operation && operation.id === c.req.param("operationId"))
          target = operation.commit?.targetPackKey;
      } else {
        const ordinal = c.req.param("ordinal") ?? "";
        if (!/^(?:0|[1-9][0-9]{0,2})$/.test(ordinal) || Number(ordinal) >= 250) {
          await release();
          return new Response("Not found\n", { status: 404 });
        }
        const packs = await c.var.limiter.run("do:qualification-artifact-source", () =>
          stub.getActivePackCatalog()
        );
        target = packs[Number(ordinal)]?.packKey;
      }
      if (!target) {
        await release();
        return new Response("Not published\n", { status: 409 });
      }
      const published = await readPublishedRepositoryGenerationState({
        env: c.env,
        doId: stub.id.toString(),
        limiter: c.var.limiter,
        countSubrequest: () => {},
      });
      if (
        !published?.activePackKeys.has(target) ||
        (!c.req.param("operationId") && c.req.query("generation") !== String(published.generation))
      ) {
        await release();
        return new Response("Not published\n", { status: 409 });
      }
      const key =
        role === "pack" ? target : role === "index" ? packIndexKey(target) : packRefsKey(target);
      const object = await c.var.limiter.run("r2:qualification-artifact", () =>
        c.env.REPO_BUCKET.get(key)
      );
      if (!object) {
        await release();
        return new Response("Artifact missing\n", { status: 409 });
      }
      const reader = object.body.getReader();
      const body = new ReadableStream<Uint8Array>({
        async pull(controller) {
          try {
            if (heartbeatFailure) throw heartbeatFailure;
            const next = await reader.read();
            if (next.done) {
              await release();
              controller.close();
            } else controller.enqueue(next.value);
          } catch (error) {
            await reader.cancel().catch(() => {});
            await release();
            controller.error(error);
          }
        },
        async cancel() {
          await reader.cancel();
          await release();
        },
      });
      return new Response(body, {
        headers: {
          "Content-Type": "application/octet-stream",
          "Content-Length": String(object.size),
          "Cache-Control": "no-store",
        },
      });
    } catch (error) {
      await release();
      throw error;
    }
  };
  router.get("/_internal/qualification/:owner/:repo/gc/:operationId/artifacts/:role", artifact);
  router.get("/_internal/qualification/:owner/:repo/gc-source/:ordinal/artifacts/:role", artifact);
}
