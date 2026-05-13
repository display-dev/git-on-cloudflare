export type {
  CommitDiffChangeType,
  CommitDiffEntry,
  CommitDiffResult,
  CommitFilePatchResult,
} from "@/shared/git/types";

export interface TreeEntry {
  /** Git tree mode (e.g., "100644" for regular file, "120000" for symlink, "40000" for directory) */
  mode: string;
  /** Entry name (filename or directory name) */
  name: string;
  /** Object ID of the blob (file/symlink) or tree (directory) */
  oid: string;
}

export interface CommitInfo {
  oid: string;
  tree: string;
  parents: string[];
  author?: { name: string; email: string; when: number; tz: string };
  committer?: { name: string; email: string; when: number; tz: string };
  message: string;
}

export interface MergeSideOptions {
  /** Maximum commits to scan before stopping (default: limit * 3) */
  scanLimit?: number;
  /** Time budget in milliseconds before stopping (default: 150ms) */
  timeBudgetMs?: number;
  /** Number of mainline commits to probe for early stop (default: 300) */
  mainlineProbe?: number;
}
