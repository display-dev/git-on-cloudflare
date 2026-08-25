import type { StockReceiveContainerHost } from "./do/stockReceiveContainerHost";

declare global {
  namespace Cloudflare {
    interface Env {
      STOCK_RECEIVE_CONTAINER_HOST: DurableObjectNamespace<StockReceiveContainerHost>;
    }
  }
}

export {};
