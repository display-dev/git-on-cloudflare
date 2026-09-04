import { hexToBytes } from "@/worker/common";
import { concatChunks } from "@/worker/git/core/pktline";
import { computeOid, type GitObjectType } from "@/worker/git/core/objects";
import { buildPackV2Artifacts, type PackV2Artifacts } from "@/worker/git/pack/build";

export type IngestionFile = {
  path: string;
  bytes: Uint8Array;
};

export type BuiltIngestionCommit = PackV2Artifacts & {
  commitOid: string;
  treeOid: string;
  objectCount: number;
};

type PackObject = {
  type: GitObjectType;
  oid: string;
  payload: Uint8Array;
};

type TreeLeaf = {
  kind: "file";
  oid: string;
};

type TreeDirectory = {
  kind: "directory";
  entries: Map<string, TreeNode>;
};

type TreeNode = TreeLeaf | TreeDirectory;

type EncodedTreeEntry = {
  name: string;
  mode: "100644" | "40000";
  oid: string;
  directory: boolean;
};

const encoder = new TextEncoder();

function compareBytes(left: Uint8Array, right: Uint8Array): number {
  const sharedLength = Math.min(left.byteLength, right.byteLength);
  for (let index = 0; index < sharedLength; index++) {
    const difference = left[index]! - right[index]!;
    if (difference !== 0) return difference;
  }
  return left.byteLength - right.byteLength;
}

function compareTreeEntries(left: EncodedTreeEntry, right: EncodedTreeEntry): number {
  // Git compares a directory as though its name has a trailing slash.
  const leftName = encoder.encode(left.directory ? `${left.name}/` : left.name);
  const rightName = encoder.encode(right.directory ? `${right.name}/` : right.name);
  return compareBytes(leftName, rightName);
}

function addFileToTree(root: TreeDirectory, path: string, oid: string): void {
  const segments = path.split("/");
  let directory = root;
  for (let index = 0; index < segments.length - 1; index++) {
    const segment = segments[index]!;
    const existing = directory.entries.get(segment);
    if (existing?.kind === "file") {
      throw new Error(`Path conflicts with a file: ${path}`);
    }
    if (existing?.kind === "directory") {
      directory = existing;
      continue;
    }
    const child: TreeDirectory = { kind: "directory", entries: new Map() };
    directory.entries.set(segment, child);
    directory = child;
  }

  const name = segments[segments.length - 1]!;
  if (directory.entries.has(name)) {
    throw new Error(`Duplicate or conflicting path: ${path}`);
  }
  directory.entries.set(name, { kind: "file", oid });
}

async function encodeTree(
  directory: TreeDirectory,
  objects: Map<string, PackObject>
): Promise<{ oid: string; payload: Uint8Array }> {
  const entries: EncodedTreeEntry[] = [];
  for (const [name, node] of directory.entries) {
    if (node.kind === "file") {
      entries.push({ name, mode: "100644", oid: node.oid, directory: false });
      continue;
    }
    const child = await encodeTree(node, objects);
    entries.push({ name, mode: "40000", oid: child.oid, directory: true });
  }

  entries.sort(compareTreeEntries);
  const parts: Uint8Array[] = [];
  for (const entry of entries) {
    parts.push(
      encoder.encode(`${entry.mode} ${entry.name}`),
      Uint8Array.of(0),
      hexToBytes(entry.oid)
    );
  }
  const payload = concatChunks(parts);
  const oid = await computeOid("tree", payload);
  objects.set(oid, { type: "tree", oid, payload });
  return { oid, payload };
}

export async function buildIngestionCommit(args: {
  files: IngestionFile[];
  parentOid: string | null;
  committedAtSeconds: number;
  message: string;
}): Promise<BuiltIngestionCommit> {
  const objects = new Map<string, PackObject>();
  const root: TreeDirectory = { kind: "directory", entries: new Map() };

  for (const file of args.files) {
    const oid = await computeOid("blob", file.bytes);
    objects.set(oid, { type: "blob", oid, payload: file.bytes });
    addFileToTree(root, file.path, oid);
  }

  const tree = await encodeTree(root, objects);
  const identity = `display.dev ingestion <ingestion@display.dev> ${args.committedAtSeconds} +0000`;
  const parentLine = args.parentOid ? `parent ${args.parentOid}\n` : "";
  const commitPayload = encoder.encode(
    `tree ${tree.oid}\n${parentLine}author ${identity}\ncommitter ${identity}\n\n${args.message}\n`
  );
  const commitOid = await computeOid("commit", commitPayload);
  objects.set(commitOid, { type: "commit", oid: commitOid, payload: commitPayload });
  const packObjects = Array.from(objects.values());

  const artifacts = await buildPackV2Artifacts(packObjects);
  return {
    commitOid,
    treeOid: tree.oid,
    ...artifacts,
    objectCount: packObjects.length,
  };
}
