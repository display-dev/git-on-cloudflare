import { DurableObject } from "cloudflare:workers";

import { createLogger } from "@/worker/common/logger";

const CONTAINER_PORT = 8080;
const READY_ATTEMPTS = 120;
const READY_INTERVAL_MS = 250;
const START_RETRY_ATTEMPTS = 20;
const DEFAULT_IDLE_RETENTION_SECONDS = 120;
const MIN_IDLE_RETENTION_SECONDS = 5;
const MAX_IDLE_RETENTION_SECONDS = 15 * 60;
const REQUEST_MAX_BYTES = 48 * 1024 * 1024 + 64 * 1024 + 12;
const RESPONSE_MAX_BYTES = 96 * 1024 * 1024 + 1024 * 1024 + 12;
const TIMING_HEADER_PREFIX = "X-Display-Stock-Container-";

type TestContainerExecutor = (request: Request) => Promise<Response>;
let testContainerExecutor: TestContainerExecutor | undefined;

type StockContainer = Pick<Container, "running" | "start" | "getTcpPort">;
type StockContainerLifecycle = Pick<Container, "setInactivityTimeout">;

type ContainerReadiness = {
  ready: boolean;
  startAttempts: number;
  probeAttempts: number;
};

type ContainerTiming = ContainerReadiness & {
  wasRunning: boolean;
  readinessMs: number;
};

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
  async readiness(container: StockContainer): Promise<ContainerReadiness> {
    return await containerReadiness(container, async () => {});
  },
  idleRetentionMs(value: string | undefined): number {
    return idleRetentionMs(value);
  },
  async configureIdleRetention(
    container: StockContainerLifecycle,
    value: string | undefined
  ): Promise<void> {
    await configureIdleRetention(container, value);
  },
  timingHeaders(timing: ContainerTiming): Headers {
    return timingHeaders(timing);
  },
  async forwardResponse(response: Response, timing: ContainerTiming): Promise<Response> {
    return await boundedContainerResponse(response, timing);
  },
};

function idleRetentionMs(value: string | undefined): number {
  if (!value || !/^\d+$/.test(value)) return DEFAULT_IDLE_RETENTION_SECONDS * 1000;
  const configuredSeconds = Number(value);
  if (!Number.isSafeInteger(configuredSeconds)) return DEFAULT_IDLE_RETENTION_SECONDS * 1000;
  const boundedSeconds = Math.min(
    MAX_IDLE_RETENTION_SECONDS,
    Math.max(MIN_IDLE_RETENTION_SECONDS, configuredSeconds)
  );
  return boundedSeconds * 1000;
}

async function configureIdleRetention(
  container: StockContainerLifecycle,
  value: string | undefined
): Promise<void> {
  await container.setInactivityTimeout(idleRetentionMs(value));
}

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
  return (await containerReadiness(container, sleep)).ready;
}

async function containerReadiness(
  container: StockContainer,
  sleep: (milliseconds: number) => Promise<void> = delay
): Promise<ContainerReadiness> {
  let lastStartAttempt: number | null = null;
  let startAttempts = 0;
  let probeAttempts = 0;
  for (let attempt = 0; attempt < READY_ATTEMPTS; attempt++) {
    // `running` is only a point-in-time process state. A Container can exit
    // after a successful start but before its port becomes ready. Reissue start
    // only while it still reports stopped and at most once every five seconds,
    // instead of polling a dead lifecycle for the rest of the existing window.
    if (
      !container.running &&
      (lastStartAttempt === null || attempt - lastStartAttempt >= START_RETRY_ATTEMPTS)
    ) {
      lastStartAttempt = attempt;
      startAttempts++;
      try {
        container.start({ enableInternet: false });
      } catch {
        // A transient platform start failure remains inside the bounded
        // readiness observation and the same relative retry cadence.
      }
    }
    try {
      probeAttempts++;
      const response = await container
        .getTcpPort(CONTAINER_PORT)
        .fetch("http://container/ready", { method: "GET" });
      await response.body?.cancel();
      if (response.ok) return { ready: true, startAttempts, probeAttempts };
    } catch {
      // Container startup failures remain inside the bounded readiness loop.
    }
    await sleep(READY_INTERVAL_MS);
  }
  return { ready: false, startAttempts, probeAttempts };
}

function timingHeaders(timing: ContainerTiming): Headers {
  return new Headers({
    [`${TIMING_HEADER_PREFIX}Was-Running`]: timing.wasRunning ? "1" : "0",
    [`${TIMING_HEADER_PREFIX}Readiness-Ms`]: String(timing.readinessMs),
    [`${TIMING_HEADER_PREFIX}Start-Attempts`]: String(timing.startAttempts),
    [`${TIMING_HEADER_PREFIX}Probe-Attempts`]: String(timing.probeAttempts),
  });
}

async function boundedContainerResponse(
  response: Response,
  timing?: ContainerTiming
): Promise<Response> {
  const responseBytes = boundedContentLength(response.headers, RESPONSE_MAX_BYTES);
  if (!response.ok || responseBytes === null || !response.body) {
    await response.body?.cancel();
    return new Response("Container rejected receive\n", {
      status: response.status >= 500 ? 503 : 422,
    });
  }
  const headers = new Headers({
    "Content-Type": "application/x-display-stock-receive-output",
    "Content-Length": String(responseBytes),
    "Cache-Control": "no-store",
  });
  if (timing) {
    for (const [name, value] of timingHeaders(timing)) headers.set(name, value);
  }
  return new Response(response.body, {
    status: 200,
    headers,
  });
}

/**
 * Zero-authority Container lifecycle host. The Worker supplies and receives
 * fixed-length framed streams. A finite inactivity timeout retains the running
 * process between nearby requests; Cloudflare still owns idle shutdown, and
 * the host keeps no R2 binding, RepoDO stub, WorkerEntrypoint, or outbound
 * interception.
 */
export class StockReceiveContainerHost extends DurableObject {
  public constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    const container = ctx.container;
    if (!container) return;

    // The platform timeout is inactivity-aware, so an in-flight receive is not
    // stopped. Failure leaves the prior immediate-inactivity behavior intact
    // and must not make the correctness path unavailable.
    ctx.blockConcurrencyWhile(async () => {
      try {
        await configureIdleRetention(container, env.STOCK_RECEIVE_CONTAINER_IDLE_SECONDS);
      } catch {
        createLogger("info", { service: "StockReceiveContainerHost" }).warn(
          "stock-container-host:idle-retention-configuration-failed",
          {}
        );
      }
    });
  }

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
      const wasRunning = container.running;
      const readinessStartedAt = Date.now();
      const readiness = await containerReadiness(container);
      const timing = {
        ...readiness,
        wasRunning,
        readinessMs: Date.now() - readinessStartedAt,
      };
      if (!readiness.ready) {
        logger.warn("stock-container-host:readiness-failed", {
          startAttempts: readiness.startAttempts,
          probeAttempts: readiness.probeAttempts,
        });
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
      return await boundedContainerResponse(response, timing);
    } catch {
      logger.warn("stock-container-host:transport-failed", { retryable: true });
      return new Response("Container unavailable\n", {
        status: 503,
        headers: { "X-Display-Stock-Container-Diagnostic": "forward-failed" },
      });
    }
  }
}
