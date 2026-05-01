// Stable public entrypoint for rewrite-planner callers and tests.

export { buildSelection, type BuildSelectionResult } from "./selection";
export { compactDeadSlots } from "./selectionCompact";
export { buildOutputOrder, canPassthroughSinglePack, computeHeaderLengths } from "./layout";
