export type RefKind = "branch" | "tag" | "other";

export type RefLike = {
  name: string;
};

export function shortRefName(refName: string): string {
  return refName.replace(/^refs\/(heads|tags)\//, "");
}

export function classifyRef(refName: string): RefKind {
  if (refName.startsWith("refs/heads/")) return "branch";
  if (refName.startsWith("refs/tags/")) return "tag";
  return "other";
}

export function formatRefOption(ref: RefLike): { name: string; displayName: string } {
  const short = shortRefName(ref.name);
  return {
    name: encodeURIComponent(short),
    displayName: short.length > 30 ? `${short.slice(0, 27)}...` : short,
  };
}

export function countRefsByKind(refs: readonly RefLike[]): {
  branchCount: number;
  tagCount: number;
  otherCount: number;
} {
  let branchCount = 0;
  let tagCount = 0;
  let otherCount = 0;

  for (const ref of refs) {
    const kind = classifyRef(ref.name);
    if (kind === "branch") branchCount++;
    else if (kind === "tag") tagCount++;
    else otherCount++;
  }

  return { branchCount, tagCount, otherCount };
}
