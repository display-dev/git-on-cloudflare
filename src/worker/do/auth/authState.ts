// Typed schema for Auth Durable Object storage
// Provides strong typing for users and rate-limit entries.

export type RateLimitKey = `ratelimit:${string}`;
export type UsersKey = "users";

export type AuthUsers = Record<string, string[]>; // owner -> array of salted+hashed tokens

export interface RateLimitEntry {
  attempts: number;
  lastAttempt: number;
  blockedUntil?: number;
}

export type AuthStateSchema = {
  users: AuthUsers;
} & Record<RateLimitKey, RateLimitEntry>;

/** Build a rate-limit key for an owner + client IP */
export function makeOwnerRateLimitKey(owner: string, clientIp: string): RateLimitKey {
  return `ratelimit:${owner}:${clientIp}` as RateLimitKey;
}

/** Build a rate-limit key for admin attempts by client IP */
export function makeAdminRateLimitKey(clientIp: string): RateLimitKey {
  return `ratelimit:admin:${clientIp}` as RateLimitKey;
}
