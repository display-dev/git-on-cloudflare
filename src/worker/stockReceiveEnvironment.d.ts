import type { StockReceiveContainerHost } from "./do/stockReceiveContainerHost";
import type { MaintenanceContainerHost } from "./do/maintenanceContainerHost";

declare global {
  namespace Cloudflare {
    interface Env {
      STOCK_RECEIVE_CONTAINER_HOST: DurableObjectNamespace<StockReceiveContainerHost>;
      MAINTENANCE_CONTAINER_HOST: DurableObjectNamespace<MaintenanceContainerHost>;
    }
  }
}

export {};
