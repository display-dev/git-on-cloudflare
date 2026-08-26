// Node-only unit tests do not run inside workerd. Production code uses this
// binding only to populate an optional execution-context fallback, so an empty
// export catalogue accurately characterizes the unit-test environment.
export const exports = {};
