import { DurableObject } from "cloudflare:workers";
import { createLogger } from "@/worker/common/logger";
import { NativeProcessorError, runContainerProcessor } from "./repo/nativeReceive";
import {
  currentNativeProcessor,
  deleteNativeProcessorSlot,
  stopNativeProcessorSlot,
} from "./nativeProcessorSlot";
import {
  nativeBridgeGrantDigest,
  type NativeExecutionIdentity,
} from "@/worker/git/nativeReceive/execution";
import type {
  NativeReceiveProcessRequest,
  NativeReceiveProcessResult,
  RepositoryContainerBridgeProps,
} from "@/worker/git/nativeReceive/types";

export type MaintenanceExecutionResult =
  | { status: "processed"; result: NativeReceiveProcessResult }
  | { status: "failed"; code: string; retryable: boolean };

/** Execution-only adapter. No ref/catalog mutations or direct R2 operations.
 * Unlike the framed-stream host, this host explicitly installs a coordinator-
 * issued exact-job bridge. The repository remains the capability authority. */
export class MaintenanceContainerHost extends DurableObject<Env> {
  async process(
    request: NativeReceiveProcessRequest,
    bridgeProps: RepositoryContainerBridgeProps,
    identity: NativeExecutionIdentity,
    onReady: (wasRunning: boolean) => Promise<void>
  ): Promise<MaintenanceExecutionResult> {
    const log = createLogger(this.env.LOG_LEVEL, { service: "MaintenanceContainerHost" });
    if (
      identity.lane !== "maintenance" ||
      !request.maintenance ||
      request.operationId !== identity.operationId ||
      this.env.MAINTENANCE_CONTAINER_HOST.idFromName(identity.repositoryId).toString() !==
        this.ctx.id.toString() ||
      (await nativeBridgeGrantDigest(bridgeProps)) !== identity.grantSha256
    ) {
      log.warn("maintenance-host:descriptor-rejected", {});
      return { status: "failed", code: "native_execution_mismatch", retryable: false };
    }
    try {
      const result = await runContainerProcessor({
        ctx: this.ctx,
        request,
        bridgeProps,
        execution: identity,
        onReady,
      });
      log.info("maintenance-host:completed", { generation: identity.generation });
      return { status: "processed", result };
    } catch (error) {
      log.warn("maintenance-host:processing-failed", { generation: identity.generation });
      return {
        status: "failed",
        code: error instanceof NativeProcessorError ? error.code : "container_transport_failure",
        retryable: error instanceof NativeProcessorError ? error.retryable : true,
      };
    }
  }

  async cancel(identity: NativeExecutionIdentity): Promise<boolean> {
    if (
      identity.lane !== "maintenance" ||
      this.env.MAINTENANCE_CONTAINER_HOST.idFromName(identity.repositoryId).toString() !==
        this.ctx.id.toString()
    )
      return false;
    const stopped = await stopNativeProcessorSlot(this.ctx, identity);
    createLogger(this.env.LOG_LEVEL, { service: "MaintenanceContainerHost" }).info(
      "maintenance-host:cancelled",
      { generation: identity.generation, stopped }
    );
    return stopped;
  }

  async deleteRepositoryExecution(): Promise<void> {
    await deleteNativeProcessorSlot(this.ctx);
    createLogger(this.env.LOG_LEVEL, { service: "MaintenanceContainerHost" }).info(
      "maintenance-host:repository-stopped",
      {}
    );
  }

  async observe(operationId: string): Promise<boolean> {
    const record = await currentNativeProcessor(this.ctx);
    if (
      record?.state !== "active" ||
      record.identity.operationId !== operationId ||
      !this.ctx.container?.running
    )
      return false;
    try {
      const response = await this.ctx.container
        .getTcpPort(8080)
        .fetch("http://container/maintenance/status", { signal: AbortSignal.timeout(5_000) });
      const status = await response.json<{ operationId: string }>();
      return status.operationId === operationId;
    } catch {
      return false;
    }
  }

  async alarm(): Promise<void> {
    const record = await currentNativeProcessor(this.ctx);
    if (record && record.identity.expiresAt > Date.now()) {
      await this.ctx.storage.setAlarm(record.identity.expiresAt);
      return;
    }
    if (record && record.identity.expiresAt <= Date.now()) {
      await stopNativeProcessorSlot(this.ctx, record.identity);
      createLogger(this.env.LOG_LEVEL, { service: "MaintenanceContainerHost" }).info(
        "maintenance-host:expired",
        { generation: record.identity.generation }
      );
    }
  }
}
