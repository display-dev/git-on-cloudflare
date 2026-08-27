import { WorkerEntrypoint } from "cloudflare:workers";

import { createLogger } from "@/worker/common/logger";
import { bytesToHex, createDigestStream } from "@/worker/common";
import { MAX_SIMULTANEOUS_CONNECTIONS, SubrequestLimiter } from "@/worker/git/operations/limits";
import type { RepositoryContainerBridgeProps } from "./types";

const BRIDGE_PATH_PREFIX = "/r2/";

function decodeKey(pathname: string): string | null {
  if (!pathname.startsWith(BRIDGE_PATH_PREFIX)) return null;
  const encoded = pathname.slice(BRIDGE_PATH_PREFIX.length);
  if (!encoded || !/^[A-Za-z0-9_-]+$/.test(encoded)) return null;
  try {
    const normalized = encoded.replace(/-/g, "+").replace(/_/g, "/");
    const padding = "=".repeat((4 - (normalized.length % 4)) % 4);
    const binary = atob(normalized + padding);
    return new TextDecoder().decode(Uint8Array.from(binary, (value) => value.charCodeAt(0)));
  } catch {
    return null;
  }
}

function contentLength(request: Request): number | null {
  const value = request.headers.get("Content-Length");
  if (!value || !/^\d+$/.test(value)) return null;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null;
}

function keyRole(props: RepositoryContainerBridgeProps, key: string): string {
  const readIndex = props.readKeys.findIndex((entry) => entry.key === key);
  if (readIndex >= 0) return readIndex === 0 ? "input" : "active-catalog";
  const writeIndex = props.writeKeys.findIndex((entry) => entry.key === key);
  return writeIndex >= 0
    ? (["output-pack", "output-index", "output-refs", "output-receipt"][writeIndex] ?? "unknown")
    : "unknown";
}

async function writeFixedLengthBody(
  body: ReadableStream<Uint8Array>,
  writable: WritableStream<Uint8Array>,
  expectedBytes: number,
  digestWriter?: WritableStreamDefaultWriter<Uint8Array>
): Promise<void> {
  const reader = body.getReader();
  const writer = writable.getWriter();
  let written = 0;
  try {
    while (true) {
      const next = await reader.read();
      if (next.done) break;
      written += next.value.byteLength;
      if (written > expectedBytes) {
        throw new Error("Container output exceeded its declared Content-Length");
      }
      await digestWriter?.write(next.value);
      await writer.write(next.value);
    }
    if (written !== expectedBytes) {
      throw new Error("Container output did not match its declared Content-Length");
    }
    await writer.close();
    await digestWriter?.close();
  } catch (error) {
    await writer.abort(error).catch(() => {});
    throw error;
  } finally {
    reader.releaseLock();
  }
}

async function hashStoredObject(
  object: R2ObjectBody,
  expectedBytes: number
): Promise<string | null> {
  if (object.size !== expectedBytes) return null;
  const digest = createDigestStream("SHA-256");
  const writer = digest.getWriter();
  const reader = object.body.getReader();
  let readBytes = 0;
  try {
    while (true) {
      const next = await reader.read();
      if (next.done) break;
      if (next.value.byteLength > expectedBytes - readBytes) return null;
      readBytes += next.value.byteLength;
      await writer.write(next.value);
    }
    if (readBytes !== expectedBytes) return null;
    await writer.close();
    return bytesToHex(new Uint8Array(await digest.digest));
  } finally {
    reader.releaseLock();
  }
}

export class RepositoryContainerBridge extends WorkerEntrypoint<
  Env,
  RepositoryContainerBridgeProps
> {
  private readonly limiter = new SubrequestLimiter(MAX_SIMULTANEOUS_CONNECTIONS);

  override async fetch(request: Request): Promise<Response> {
    const log = createLogger(this.env.LOG_LEVEL, {
      service: "RepositoryContainerBridge",
    });
    const key = decodeKey(new URL(request.url).pathname);
    if (!key) {
      log.warn("container-bridge:invalid-key", {
        operationId: this.ctx.props.operationId,
        method: request.method,
      });
      return new Response("Not found\n", { status: 404 });
    }

    if (request.method === "GET") {
      const allowance = this.ctx.props.readKeys.find((entry) => entry.key === key);
      if (!allowance) {
        log.warn("container-bridge:read-denied", {
          operationId: this.ctx.props.operationId,
          keyRole: "unknown",
        });
        return new Response("Forbidden\n", { status: 403 });
      }
      const object = await this.limiter.run("r2:container-bridge-get", () =>
        this.env.REPO_BUCKET.get(key)
      );
      if (
        !object ||
        object.size !== allowance.expectedBytes ||
        (allowance.expectedEtag !== undefined && object.etag !== allowance.expectedEtag)
      ) {
        log.warn("container-bridge:read-missing", {
          operationId: this.ctx.props.operationId,
          keyRole: keyRole(this.ctx.props, key),
        });
        return new Response("Not found\n", { status: 404 });
      }
      log.debug("container-bridge:read", {
        operationId: this.ctx.props.operationId,
        keyRole: keyRole(this.ctx.props, key),
        bytes: object.size,
      });
      return new Response(object.body, {
        headers: {
          "Content-Type": "application/octet-stream",
          "Content-Length": String(object.size),
          "Cache-Control": "no-store",
        },
      });
    }

    if (request.method === "PUT") {
      const allowance = this.ctx.props.writeKeys.find((entry) => entry.key === key);
      const bytes = contentLength(request);
      const declaredSha256 = request.headers.get("X-Display-SHA256")?.toLowerCase();
      const validDeclaredSha256 =
        declaredSha256 !== undefined && /^[0-9a-f]{64}$/.test(declaredSha256);
      if (
        !allowance ||
        bytes === null ||
        bytes > allowance.maxBytes ||
        !request.body ||
        (this.ctx.props.requireWriteSha256 && !validDeclaredSha256)
      ) {
        log.warn("container-bridge:write-denied", {
          operationId: this.ctx.props.operationId,
          keyRole: keyRole(this.ctx.props, key),
          bytes,
          allowed: allowance?.maxBytes,
        });
        return new Response("Forbidden\n", { status: 403 });
      }
      // R2 accepts streamed puts only when workerd can prove the exact length.
      // FixedLengthStream preserves that contract and rejects both a short
      // body and an overrun without buffering Container output in the Worker.
      const fixedLength = new FixedLengthStream(bytes);
      const digest = this.ctx.props.requireWriteSha256 ? createDigestStream("SHA-256") : undefined;
      const pipePromise = writeFixedLengthBody(
        request.body,
        fixedLength.writable,
        bytes,
        digest?.getWriter()
      );
      const putPromise = this.limiter.run("r2:container-bridge-put", () =>
        this.env.REPO_BUCKET.put(key, fixedLength.readable, {
          onlyIf: this.ctx.props.requireWriteSha256 ? { etagDoesNotMatch: "*" } : undefined,
          httpMetadata: { contentType: "application/octet-stream" },
          customMetadata: validDeclaredSha256 ? { sha256: declaredSha256 } : undefined,
        })
      );
      const [pipeResult, putResult] = await Promise.allSettled([pipePromise, putPromise]);
      const createdByInvocation = putResult.status === "fulfilled" && putResult.value !== null;
      if (pipeResult.status === "rejected" || putResult.status === "rejected") {
        if (createdByInvocation && !this.ctx.props.durableOutputOwner) {
          await this.limiter
            .run("r2:container-bridge-delete-rejected", () => this.env.REPO_BUCKET.delete(key))
            .catch(() => {});
        }
        const bodyMismatch = pipeResult.status === "rejected";
        log.warn("container-bridge:write-rejected", {
          operationId: this.ctx.props.operationId,
          keyRole: keyRole(this.ctx.props, key),
          reason: bodyMismatch ? "body-length" : "storage",
        });
        return new Response(bodyMismatch ? "Invalid body length\n" : "Storage unavailable\n", {
          status: bodyMismatch ? 400 : 503,
        });
      }
      let computedSha256: string | undefined;
      if (digest) {
        computedSha256 = bytesToHex(new Uint8Array(await digest.digest));
        if (computedSha256 !== declaredSha256) {
          if (createdByInvocation && !this.ctx.props.durableOutputOwner) {
            await this.limiter.run("r2:container-bridge-delete-digest-mismatch", () =>
              this.env.REPO_BUCKET.delete(key)
            );
          }
          return new Response("Write digest mismatch\n", { status: 400 });
        }
      }
      if (this.ctx.props.requireWriteSha256 && putResult.value === null) {
        const existing = await this.limiter.run("r2:container-bridge-get-existing", () =>
          this.env.REPO_BUCKET.get(key)
        );
        const existingSha256 = existing ? await hashStoredObject(existing, bytes) : null;
        if (
          !existing ||
          existingSha256 !== computedSha256 ||
          existing.customMetadata?.sha256 !== declaredSha256
        ) {
          log.warn("container-bridge:immutable-write-conflict", {
            operationId: this.ctx.props.operationId,
            keyRole: keyRole(this.ctx.props, key),
            bytes,
          });
          return new Response("Immutable output key already exists\n", { status: 409 });
        }
        log.info("container-bridge:write-reconciled", {
          operationId: this.ctx.props.operationId,
          keyRole: keyRole(this.ctx.props, key),
          bytes,
        });
        return new Response(null, {
          status: 204,
          headers: { ETag: existing.etag },
        });
      }
      const stored = await this.limiter.run("r2:container-bridge-head", () =>
        this.env.REPO_BUCKET.head(key)
      );
      if (
        !stored ||
        stored.size !== bytes ||
        (this.ctx.props.requireWriteSha256 && stored.customMetadata?.sha256 !== declaredSha256)
      ) {
        if (createdByInvocation && !this.ctx.props.durableOutputOwner) {
          await this.limiter.run("r2:container-bridge-delete-invalid", () =>
            this.env.REPO_BUCKET.delete(key)
          );
        }
        log.error("container-bridge:write-verification-failed", {
          operationId: this.ctx.props.operationId,
          keyRole: keyRole(this.ctx.props, key),
          expectedBytes: bytes,
          actualBytes: stored?.size,
        });
        return new Response("Write verification failed\n", { status: 502 });
      }
      log.info("container-bridge:write", {
        operationId: this.ctx.props.operationId,
        keyRole: keyRole(this.ctx.props, key),
        bytes,
      });
      return new Response(null, {
        status: 204,
        headers: stored ? { ETag: stored.etag } : undefined,
      });
    }

    return new Response("Method not allowed\n", {
      status: 405,
      headers: { Allow: "GET, PUT" },
    });
  }
}
