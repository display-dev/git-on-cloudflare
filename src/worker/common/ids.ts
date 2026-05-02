// Generate a prefixed identifier with 16 random bytes encoded as hex.
// Used for D1 row primary keys: `user_<32 hex>`, `ns_<32 hex>`,
// `repo_<32 hex>`, `pat_<32 hex>`. The hex tail is ~128 bits, enough
// entropy to make collisions practically impossible without depending
// on D1 retry semantics for unique-key conflicts.
export function newPrefixedId(prefix: string): string {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  let hex = "";
  for (let i = 0; i < bytes.length; i += 1) {
    hex += bytes[i]!.toString(16).padStart(2, "0");
  }
  return `${prefix}_${hex}`;
}
