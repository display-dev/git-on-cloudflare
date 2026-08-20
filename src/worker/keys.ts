// Centralized helpers for building repo IDs and R2/DO key paths
// Keep all key/prefix formats here to avoid divergence between tests and runtime.

// Compute the Durable Object R2 prefix for a given DO instance ID.
// DO-backed data in R2 is stored under: "do/<durable-object-id>/..."
export function doPrefix(doId: string): string {
  return `do/${doId}`;
}

// R2 key for a loose object under a given DO prefix
export function r2LooseKey(prefix: string, oid: string): string {
  return `${prefix}/objects/loose/${oid}`;
}

// R2 key for a pack file under a given DO prefix
export function r2PackKey(prefix: string, name: string): string {
  return `${prefix}/objects/pack/${name}`;
}

// Given a .pack key, return its matching .idx key
export function packIndexKey(packKey: string): string {
  return packKey.replace(/\.pack$/, ".idx");
}

// Given a .pack key, return its matching logical-reference sidecar key.
export function packRefsKey(packKey: string): string {
  return packKey.replace(/\.pack$/, ".refs");
}

// Directory prefix for pack objects under a given DO prefix
export function r2PackDirPrefix(prefix: string): string {
  return `${prefix}/objects/pack/`;
}

// The raw client pack is immutable staging input. Native processing writes a
// separate self-contained pack so a retry can safely replace only its own
// derived artifacts without ever mutating an active catalog entry.
export function nativeReceiveInputPackKey(prefix: string, operationId: string): string {
  return r2PackKey(prefix, `pack-native-input-${operationId}.pack`);
}

export function nativeReceiveOutputPackKey(
  prefix: string,
  operationId: string,
  fingerprint: string
): string {
  return r2PackKey(prefix, `pack-native-${operationId}-${fingerprint}.pack`);
}

export function repositoryImportPackKey(repositoryId: string, operationId: string): string {
  return `${repositoryImportPrefix(repositoryId)}${operationId}.pack`;
}

export function repositoryImportPrefix(repositoryId: string): string {
  return `imports/${encodeURIComponent(repositoryId)}/`;
}

// Return true if the key ends with .pack
export function isPackKey(key: string): boolean {
  return key.endsWith(".pack");
}

// Return true if the key ends with .idx
export function isIdxKey(key: string): boolean {
  return key.endsWith(".idx");
}

// Given an .idx key, return the corresponding .pack key
export function packKeyFromIndexKey(idxKey: string): string {
  return idxKey.replace(/\.idx$/, ".pack");
}

// Extract the Durable Object ID from a path starting with "do/<id>/..."
// Returns null when the path does not include the expected prefix.
export function getDoIdFromPath(path: string): string | null {
  const m = /^do\/([^/]+)/.exec(path);
  return m ? m[1] : null;
}
