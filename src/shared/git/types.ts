export type PackRefIndexStatus = "present" | "missing" | "unknown";

export type CommitDiffChangeType = "A" | "M" | "D";

export interface CommitDiffEntry {
  path: string;
  changeType: CommitDiffChangeType;
  oldOid?: string;
  newOid?: string;
  oldMode?: string;
  newMode?: string;
}

export interface CommitDiffResult {
  baseCommitOid?: string;
  compareMode: "root" | "first-parent";
  entries: CommitDiffEntry[];
  added: number;
  modified: number;
  deleted: number;
  total: number;
  truncated: boolean;
  truncateReason?: "max_files" | "max_tree_pairs" | "time_budget" | "soft_budget";
}

export interface CommitFilePatchResult {
  path: string;
  changeType: CommitDiffChangeType;
  oldOid?: string;
  newOid?: string;
  oldTooLarge?: boolean;
  newTooLarge?: boolean;
  binary?: boolean;
  skipped?: boolean;
  skipReason?: "binary" | "too_large" | "not_found" | "too_many_lines";
  patch?: string;
}
