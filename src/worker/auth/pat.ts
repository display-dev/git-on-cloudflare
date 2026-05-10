import { createDb, type Db } from "@/worker/db/d1/client";
import {
  findPatByPrefix,
  findPatGrantForNamespace,
  findPatGrantForRepo,
  type PatGrantLevel,
} from "@/worker/db/d1/dal/tokens";
import { findMembership, findNamespaceBySlug } from "@/worker/db/d1/dal/namespaces";

// Parse/generate/hash/validate helpers and `verifyPat` share a single file
// so the management and verification surfaces stay on the same parsing and
// hashing rules.

const PAT_PREFIX_PUBLIC = "goc_";
const PAT_PREFIX_HEX_LENGTH = 8;
// 32-char base32 secret, 5 bits per char => 160 bits of entropy.
const PAT_SECRET_BASE32_LENGTH = 32;
const BASE32_ALPHABET = "abcdefghijklmnopqrstuvwxyz234567";

const PAT_PATTERN = /^goc_([0-9a-f]{8})_([abcdefghijklmnopqrstuvwxyz234567]{32})$/;

export function formatPatPublicPrefix(hexPrefix: string): string {
  return `${PAT_PREFIX_PUBLIC}${hexPrefix}`;
}

function bytesToHex(bytes: Uint8Array): string {
  let out = "";
  for (let i = 0; i < bytes.length; i += 1) {
    out += bytes[i]!.toString(16).padStart(2, "0");
  }
  return out;
}

function bytesToBase32(bytes: Uint8Array): string {
  let bits = 0;
  let value = 0;
  let out = "";
  for (let i = 0; i < bytes.length; i += 1) {
    value = (value << 8) | bytes[i]!;
    bits += 8;
    while (bits >= 5) {
      bits -= 5;
      out += BASE32_ALPHABET[(value >>> bits) & 0x1f];
    }
  }
  if (bits > 0) {
    out += BASE32_ALPHABET[(value << (5 - bits)) & 0x1f];
  }
  return out;
}

export type GeneratedPat = {
  plaintext: string;
  publicPrefix: string;
};

// Mint a fresh PAT plaintext + its public prefix. The hex prefix lookups
// the row; the secret is what we hash. The full plaintext (prefix +
// secret) is what callers must hash via `hashPatPlaintext`.
export function generatePatPlaintext(): GeneratedPat {
  const prefixBytes = new Uint8Array(PAT_PREFIX_HEX_LENGTH / 2);
  crypto.getRandomValues(prefixBytes);
  // base32 needs ceil(secretLength * 5 / 8) bytes of entropy. 32 chars * 5 bits = 160 bits = 20 bytes.
  const secretBytes = new Uint8Array(20);
  crypto.getRandomValues(secretBytes);
  const hexPrefix = bytesToHex(prefixBytes);
  const secret = bytesToBase32(secretBytes).slice(0, PAT_SECRET_BASE32_LENGTH);
  const publicPrefix = formatPatPublicPrefix(hexPrefix);
  return { plaintext: `${publicPrefix}_${secret}`, publicPrefix };
}

export type ParsePatResult =
  | { ok: true; publicPrefix: string; secret: string }
  | { ok: false; reason: "malformed" };

// Validate basic shape and split a plaintext into its public prefix + secret
// segments. We never compare segments; the verifier hashes the full
// plaintext and compares against the stored hash.
export function parsePatPlaintext(plaintext: string): ParsePatResult {
  if (typeof plaintext !== "string") return { ok: false, reason: "malformed" };
  const match = PAT_PATTERN.exec(plaintext);
  if (!match) return { ok: false, reason: "malformed" };
  return {
    ok: true,
    publicPrefix: formatPatPublicPrefix(match[1]!),
    secret: match[2]!,
  };
}

export async function hashPatPlaintext(plaintext: string): Promise<string> {
  const data = new TextEncoder().encode(plaintext);
  const digest = await crypto.subtle.digest("SHA-256", data);
  return bytesToHex(new Uint8Array(digest));
}

const PAT_NAME_PATTERN = /^[A-Za-z0-9](?:[A-Za-z0-9 _-]{0,38}[A-Za-z0-9])?$/;

export type PatNameValidation =
  | { ok: true; name: string }
  | { ok: false; reason: "format" | "length" };

export function validatePatName(input: string): PatNameValidation {
  if (typeof input !== "string") return { ok: false, reason: "format" };
  const trimmed = input.trim();
  if (trimmed.length === 0 || trimmed.length > 40) return { ok: false, reason: "length" };
  if (!PAT_NAME_PATTERN.test(trimmed)) return { ok: false, reason: "format" };
  return { ok: true, name: trimmed };
}

export type PatVerifyOk = {
  ok: true;
  patId: string;
  userId: string;
  namespaceId: string;
  repositoryId?: string;
  // `level === "push"` authorizes receive-pack; any present grant
  // authorizes fetch/clone because `push` includes `pull` by construction
  // (DB CHECK constraint on `pat_*_grants.level`).
  level: PatGrantLevel;
  // Loaded value at verification time so callers throttle the write
  // decision in Worker memory; see `shouldTouchPatLastUsedAt`.
  lastUsedAt: number | null;
};

export type PatVerifyError =
  | { ok: false; reason: "malformed" }
  | { ok: false; reason: "username-mismatch" }
  | { ok: false; reason: "token-not-found" }
  | { ok: false; reason: "token-revoked" }
  | { ok: false; reason: "token-expired" }
  | { ok: false; reason: "grant-missing" };

export type PatVerifyResult = PatVerifyOk | PatVerifyError;

async function constantTimeEquals(a: string, b: string): Promise<boolean> {
  if (a.length !== b.length) return false;
  const encoder = new TextEncoder();
  const aBytes = encoder.encode(a);
  const bBytes = encoder.encode(b);
  // Prefer Cloudflare's timingSafeEqual when present; fall back to a tight
  // JS XOR so this still works in non-Workers vitest unit-only contexts.
  type CfSubtle = SubtleCrypto & {
    timingSafeEqual?: (
      left: ArrayBuffer | ArrayBufferView,
      right: ArrayBuffer | ArrayBufferView
    ) => boolean;
  };
  const subtle = crypto.subtle as CfSubtle;
  if (typeof subtle.timingSafeEqual === "function") {
    return subtle.timingSafeEqual(aBytes, bBytes);
  }
  let result = 0;
  for (let i = 0; i < aBytes.length; i += 1) {
    result |= aBytes[i]! ^ bBytes[i]!;
  }
  return result === 0;
}

export type VerifyPatArgs = {
  username: string;
  plaintext: string;
  // Resolved route context supplies these ids before verification. The
  // verifier does not infer repository identity from storage names.
  namespaceId?: string;
  repositoryId?: string;
  now?: number;
  db?: Db;
};

// Verify a PAT against the stored hash and grants. Returns the user/scope
// when the token is currently valid for the requested namespace + repo
// pair, otherwise a tagged failure that the caller maps to an HTTP
// status (typically 401 for malformed/not-found, 403 for grant-missing).
export async function verifyPat(env: Env, args: VerifyPatArgs): Promise<PatVerifyResult> {
  const now = args.now ?? Date.now();
  const parsed = parsePatPlaintext(args.plaintext);
  if (!parsed.ok) return { ok: false, reason: "malformed" };

  const db = args.db ?? createDb(env.DB);

  const pat = await findPatByPrefix(db, parsed.publicPrefix);
  if (!pat) return { ok: false, reason: "token-not-found" };
  if (pat.revokedAt !== null) return { ok: false, reason: "token-revoked" };
  if (pat.expiresAt !== null && pat.expiresAt <= now) return { ok: false, reason: "token-expired" };

  const expectedHash = pat.hash;
  const presentedHash = await hashPatPlaintext(args.plaintext);
  if (!(await constantTimeEquals(expectedHash, presentedHash))) {
    return { ok: false, reason: "token-not-found" };
  }

  // Resolve namespace either directly via id or by username; both must align
  // for the username-as-namespace-slug Git authentication contract.
  let resolvedNamespaceId: string | undefined = args.namespaceId;
  let resolvedRepositoryId: string | undefined = args.repositoryId;
  if (!resolvedNamespaceId) {
    const namespace = await findNamespaceBySlug(db, args.username);
    if (!namespace) return { ok: false, reason: "username-mismatch" };
    resolvedNamespaceId = namespace.id;
  } else {
    const namespace = await findNamespaceBySlug(db, args.username);
    if (!namespace || namespace.id !== resolvedNamespaceId) {
      return { ok: false, reason: "username-mismatch" };
    }
  }

  // Repo grant takes precedence; namespace grant covers remaining repos.
  if (resolvedRepositoryId) {
    const repoGrant = await findPatGrantForRepo(db, pat.id, resolvedRepositoryId);
    if (repoGrant) {
      return {
        ok: true,
        patId: pat.id,
        userId: pat.userId,
        namespaceId: resolvedNamespaceId,
        repositoryId: resolvedRepositoryId,
        level: repoGrant.level,
        lastUsedAt: pat.lastUsedAt,
      };
    }
  }
  const namespaceGrant = await findPatGrantForNamespace(db, pat.id, resolvedNamespaceId);
  if (namespaceGrant) {
    return {
      ok: true,
      patId: pat.id,
      userId: pat.userId,
      namespaceId: resolvedNamespaceId,
      repositoryId: resolvedRepositoryId,
      level: namespaceGrant.level,
      lastUsedAt: pat.lastUsedAt,
    };
  }

  return { ok: false, reason: "grant-missing" };
}

// Writes always update; reads only when the prior value is older than this
// window (or null) so D1 is not written on every clone.
export const PAT_LAST_USED_READ_THROTTLE_MS = 15 * 60 * 1000;

export type PatLastUsedOp = "read" | "write";

export function shouldTouchPatLastUsedAt(
  lastUsedAt: number | null,
  op: PatLastUsedOp,
  now: number = Date.now()
): boolean {
  if (op === "write") return true;
  if (lastUsedAt === null) return true;
  return now - lastUsedAt >= PAT_LAST_USED_READ_THROTTLE_MS;
}

// Internal: callers in management endpoints need to confirm the
// authenticated viewer is a member of the namespace they want to scope a
// PAT to. Wraps the DAL composite-key lookup.
export async function viewerIsNamespaceMember(
  db: Db,
  userId: string,
  namespaceId: string
): Promise<boolean> {
  return (await findMembership(db, namespaceId, userId)) !== undefined;
}
