/**
 * Request/route validation helpers used across UI routes.
 */
export const MAX_REF_LEN = 256;
export const MAX_PATH_LEN = 4096;
export const OWNER_REPO_RE = /^[A-Za-z0-9._-]{1,100}$/;
export const REF_RE = /^[A-Za-z0-9._\/-]{1,256}$/; // allow refs/heads/main, tags, simple branch names
export const OID_RE = /^[0-9a-fA-F]{40}$/;

export function isValidOwnerRepo(s: string): boolean {
  return OWNER_REPO_RE.test(s);
}

export function isValidRef(ref: string): boolean {
  if (!ref || ref.length > MAX_REF_LEN) return false;
  if (ref === "HEAD") return true;
  if (OID_RE.test(ref)) return true;
  return REF_RE.test(ref);
}

export function isValidPath(p: string): boolean {
  if (p.length > MAX_PATH_LEN) return false;
  // disallow control chars and NUL
  return !/[\0\u0000-\u001F]/.test(p);
}
