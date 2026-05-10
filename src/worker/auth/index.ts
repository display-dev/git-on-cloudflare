/**
 * Authentication module barrel.
 *
 * Browser surfaces (admin pages, account UI, mutating JSON routes) authorize
 * via `session.ts` + `sessionMembership.ts`. Git endpoints authorize via
 * `gitAuth.ts` (PATs over HTTP Basic). PAT credentials must never reach
 * browser surfaces; the structural rule is that UI route modules import
 * only from `session*` / `sessionMembership`, never from `gitAuth`.
 */

export * from "./gitAuth";
