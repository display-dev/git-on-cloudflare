import { DurableObject } from "cloudflare:workers";

import { createLogger } from "@/worker/common/logger";

const CONTAINER_PORT = 8080;
const READY_ATTEMPTS = 120;
const READY_INTERVAL_MS = 250;
const REQUEST_MAX_BYTES = 48 * 1024 * 1024 + 64 * 1024 + 12;
const RESPONSE_MAX_BYTES = 96 * 1024 * 1024 + 1024 * 1024 + 12;

type TestContainerExecutor = (request: Request) => Promise<Response>;
let testContainerExecutor: TestContainerExecutor | undefined;

type StockContainer = Pick<Container, "running" | "start" | "getTcpPort">;

export const __test = {
  setContainerExecutor(executor: TestContainerExecutor): void {
    testContainerExecutor = executor;
  },
  reset(): void {
    testContainerExecutor = undefined;
  },
  async waitForReady(container: StockContainer): Promise<boolean> {
    return await waitForReady(container, async () => {});
  },
};

function boundedContentLength(headers: Headers, maximum: number): number | null {
  const value = headers.get("Content-Length");
  if (!value || !/^\d+$/.test(value)) return null;
  const bytes = Number(value);
  return Number.isSafeInteger(bytes) && bytes > 0 && bytes <= maximum ? bytes : null;
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function waitForReady(
  container: StockContainer,
  sleep: (milliseconds: number) => Promise<void> = delay
): Promise<boolean> {
  let startPending = false;
  for (let attempt = 0; attempt < READY_ATTEMPTS; attempt++) {
    // `running` is only a point-in-time process state. A Container can exit
    // after admission but before its port becomes ready; restart that lifecycle
    // before any receive bytes are forwarded instead of polling a dead port for
    // the remainder of the readiness window.
    if (!container.running && !startPending) {
      startPending = true;
      try {
        container.start({ enableInternet: false });
      } catch {
        // A concurrent platform start remains inside the bounded readiness
        // observation. Permanent startup failure becomes readiness-failed.
      }
    } else if (container.running) {
      startPending = false;
    }
    try {
      const response = await container
        .getTcpPort(CONTAINER_PORT)
        .fetch("http://container/ready", { method: "GET" });
      await response.body?.cancel();
      if (response.ok) return true;
    } catch {
      // Container startup failures remain inside the bounded readiness loop.
    }
    await sleep(READY_INTERVAL_MS);
  }
  return false;
}

async function boundedContainerResponse(response: Response): Promise<Response> {
  const responseBytes = boundedContentLength(response.headers, RESPONSE_MAX_BYTES);
  if (!response.ok || responseBytes === null || !response.body) {
    await response.body?.cancel();
    return new Response("Container rejected receive\n", {
      status: response.status >= 500 ? 503 : 422,
    });
  }
  return new Response(response.body, {
    status: 200,
    headers: {
      "Content-Type": "application/x-display-stock-receive-output",
      "Content-Length": String(responseBytes),
      "Cache-Control": "no-store",
    },
  });
}

/**
 * Zero-authority Container lifecycle host. The Worker supplies and receives
 * fixed-length framed streams. This module intentionally has no Env field,
 * R2 binding, RepoDO stub, WorkerEntrypoint, or outbound interception.
 */
export class StockReceiveContainerHost extends DurableObject {
  public async processStockReceive(request: Request): Promise<Response> {
    const logger = createLogger("info", { service: "StockReceiveContainerHost" });
    const requestBytes = boundedContentLength(request.headers, REQUEST_MAX_BYTES);
    if (
      request.method !== "POST" ||
      new URL(request.url).pathname !== "/stock-receive-bundle" ||
      requestBytes === null ||
      !request.body
    ) {
      logger.warn("stock-container-host:request-rejected", {});
      return new Response("Bad request\n", { status: 400 });
    }
    try {
      if (testContainerExecutor) {
        const response = await testContainerExecutor(request);
        return await boundedContainerResponse(response);
      }
      const container = this.ctx.container;
      if (!container) return new Response("Container unavailable\n", { status: 503 });
      if (!(await waitForReady(container))) {
        logger.warn("stock-container-host:readiness-failed", {});
        return new Response("Container unavailable\n", {
          status: 503,
          headers: { "X-Display-Stock-Container-Diagnostic": "readiness-failed" },
        });
      }
      const response = await container
        .getTcpPort(CONTAINER_PORT)
        .fetch("http://container/stock-receive-bundle", {
          method: "POST",
          headers: {
            "Content-Type": "application/x-display-stock-receive-bundle",
            "Content-Length": String(requestBytes),
          },
          body: request.body,
        });
      const responseBytes = boundedContentLength(response.headers, RESPONSE_MAX_BYTES);
      logger.info("stock-container-host:stream-forwarded", {
        requestBytes,
        responseBytes,
        responseStatus: response.status,
      });
      return await boundedContainerResponse(response);
    } catch {
      logger.warn("stock-container-host:transport-failed", { retryable: true });
      return new Response("Container unavailable\n", {
        status: 503,
        headers: { "X-Display-Stock-Container-Diagnostic": "forward-failed" },
      });
    }
  }
}
