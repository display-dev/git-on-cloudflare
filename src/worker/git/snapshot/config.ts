export function snapshotEventProbeEnabled(env: Env): boolean {
  return env.SNAPSHOT_EVENT_PROBE === "1" && Boolean(env.SNAPSHOT_BENCHMARK_PREFIX?.trim());
}
