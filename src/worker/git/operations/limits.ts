import type { CacheContext } from "@/worker/cache";

// Global caps
export const MAX_SIMULTANEOUS_CONNECTIONS = 6; // Cloudflare per-request connection limit
export const DEFAULT_SUBREQUEST_BUDGET = 900; // soft budget before hard cap (~1000)

// Structural limiter type compatible with RequestMemo.limiter
export type Limiter = { run<T>(label: string, fn: () => Promise<T>): Promise<T> };

// Lightweight semaphore to cap concurrent upstream calls per request
export class SubrequestLimiter {
  private max: number;
  private cur = 0;
  private queue: Array<() => void> = [];

  constructor(max: number) {
    this.max = Math.max(1, max | 0);
  }

  private acquire(): Promise<void> {
    if (this.cur < this.max) {
      this.cur++;
      return Promise.resolve();
    }
    return new Promise((resolve) => {
      this.queue.push(() => {
        this.cur++;
        resolve();
      });
    });
  }

  private release() {
    this.cur--;
    if (this.cur < 0) this.cur = 0;
    const next = this.queue.shift();
    if (next) next();
  }

  async run<T>(_label: string, fn: () => Promise<T>): Promise<T> {
    await this.acquire();
    try {
      return await fn();
    } finally {
      this.release();
    }
  }
}

export function getLimiter(cacheCtx?: CacheContext): Limiter {
  const fallback: Limiter = new SubrequestLimiter(MAX_SIMULTANEOUS_CONNECTIONS);
  if (!cacheCtx) return fallback as Limiter;
  cacheCtx.memo = cacheCtx.memo || {};
  if (!cacheCtx.memo.limiter) {
    cacheCtx.memo.limiter = new SubrequestLimiter(MAX_SIMULTANEOUS_CONNECTIONS);
  }
  return cacheCtx.memo.limiter as Limiter;
}

// Decrement subrequest soft budget; returns false when budget exhausted
export function countSubrequest(cacheCtx?: CacheContext, n = 1): boolean {
  if (!cacheCtx) return true; // nothing to track
  cacheCtx.memo = cacheCtx.memo || {};
  const cur = cacheCtx.memo.subreqBudget ?? DEFAULT_SUBREQUEST_BUDGET;
  const next = cur - Math.max(1, n);
  cacheCtx.memo.subreqBudget = next;
  return next >= 0;
}
