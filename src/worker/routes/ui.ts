import { AutoRouter } from "itty-router";
import { handleAdminPage } from "./ui/adminPage";
import { handleOwnerOverview, handleRepoOverview } from "./ui/overview";
import { handleTree } from "./ui/tree";
import { handleBlob } from "./ui/blob";
import { handleCommits, handleCommitFragments, handleCommitDiff, handleCommit } from "./ui/commits";
import { handleRaw, handleRawPath } from "./ui/raw";
import { handleRefsApi } from "./ui/refsApi";

export function registerUiRoutes(router: ReturnType<typeof AutoRouter>) {
  // Owner repos list
  router.get(`/:owner`, handleOwnerOverview);
  // Repo overview page
  router.get(`/:owner/:repo`, handleRepoOverview);

  // Tree/Blob browser using query params: ?ref=<branch|tag|oid>&path=<path>
  router.get(`/:owner/:repo/tree`, handleTree);

  // Blob preview endpoint - renders file content with syntax highlighting and media previews
  router.get(`/:owner/:repo/blob`, handleBlob);

  // Commit list
  router.get(`/:owner/:repo/commits`, handleCommits);

  // Merge expansion fragment endpoint: returns JSON for side-branch commits of a merge
  // Example: /:owner/:repo/commits/fragments/:oid?limit=20
  router.get(`/:owner/:repo/commits/fragments/:oid`, handleCommitFragments);

  // Commit details
  router.get(`/:owner/:repo/commit/:oid/diff`, handleCommitDiff);

  router.get(`/:owner/:repo/commit/:oid`, handleCommit);

  // Raw blob endpoint - streams file content without buffering
  router.get(`/:owner/:repo/raw`, handleRaw);

  // Raw blob by ref+path (used for images in Markdown)
  router.get(`/:owner/:repo/rawpath`, handleRawPath);

  // Async refs API for repo_nav dropdown
  router.get(`/:owner/:repo/api/refs`, handleRefsApi);

  // Admin dashboard for repository management
  router.get(`/:owner/:repo/admin`, handleAdminPage);
}
