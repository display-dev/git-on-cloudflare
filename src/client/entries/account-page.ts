import { initRepositoriesIsland } from "@/client/islands/repositories";
import { initTokensIsland } from "@/client/islands/tokens";
import { onReady } from "../on-ready";

onReady(() => {
  initRepositoriesIsland();
  initTokensIsland();
});
