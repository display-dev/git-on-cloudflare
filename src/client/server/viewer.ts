// Browser-safe viewer shape used by the SSR shell. The Worker computes this
// from the session cookie + D1 in `src/worker/auth/session.ts` and threads
// it into `renderUiDocumentResponse`. SSR components must not reach for
// session state directly — keep this presentational.
export type Viewer = {
  userId: string;
  // Slug of the user's "home" namespace. Optional because a user can exist
  // without a namespace if their `preferred_username` claim was invalid
  // or already taken when they first signed in.
  primaryNamespaceSlug?: string;
};
