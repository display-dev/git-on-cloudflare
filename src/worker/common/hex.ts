/**
 * Hex encoding/decoding utilities for Git object IDs
 */

/**
 * Convert bytes to lowercase hex string
 * @param bytes - Uint8Array to convert
 * @returns Lowercase hex string
 */
export function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

/**
 * Convert hex string to bytes
 * @param hex - Hex string (case insensitive)
 * @returns Uint8Array of bytes
 */
export function hexToBytes(hex: string): Uint8Array {
  if (hex.length % 2 !== 0) {
    throw new Error("Invalid hex string: odd length");
  }
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < hex.length; i += 2) {
    bytes[i / 2] = parseInt(hex.substr(i, 2), 16);
  }
  return bytes;
}

/**
 * Check if a string is a valid Git OID (40 hex chars)
 * @param oid - String to check
 * @returns true if valid Git OID
 */
export function isValidOid(oid: string): boolean {
  return /^[0-9a-f]{40}$/i.test(oid);
}

/**
 * Get the zero OID (40 zeros)
 * @returns "0000000000000000000000000000000000000000"
 */
export function zeroOid(): string {
  return "0".repeat(40);
}
