// Cache-policy discriminator used by visibility-aware request paths to
// decide whether the shared Workers Cache may be read or written. Public
// repositories cache freely; private repositories must bypass shared
// cache reads/writes regardless of upstream success. Repo-serving
// handlers do not import this module today; the serving-path-invariants
// worker test enforces that rule.
export type SharedCachePolicy = "allow-shared-cache" | "bypass-shared-cache";
