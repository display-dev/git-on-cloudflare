import { assert, test } from "vitest";
import {
  inflateAndParseHeader,
  parseCommitRefs,
  parseTreeChildOids,
  parseTagTarget,
} from "@/worker/git/core";
import { encodeGitObject, type GitObjectType } from "@/worker/git/core/objects";
import { deflate } from "@/worker/common/compression";

function hexToBytes(hex: string): Uint8Array {
  const out = new Uint8Array(20);
  for (let i = 0; i < 20; i++) out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  return out;
}

type CommitParts = { tree: string; parents?: string[]; message?: string };
type TagParts = {
  targetOid: string;
  targetType?: "commit" | "tree" | "blob" | "tag";
  tag?: string;
  tagger?: string;
  message?: string;
};
type BlobParts = { text?: string };

function textPayloadFor(type: "commit", parts: CommitParts): Uint8Array;
function textPayloadFor(type: "tag", parts: TagParts): Uint8Array;
function textPayloadFor(type: "blob" | "tree", parts: BlobParts): Uint8Array;
function textPayloadFor(
  type: GitObjectType,
  parts: CommitParts | TagParts | BlobParts
): Uint8Array {
  const enc = new TextEncoder();
  switch (type) {
    case "commit": {
      const { tree, parents = [], message = "" } = parts as CommitParts;
      const parentLines = parents.map((p) => `parent ${p}\n`).join("");
      const body = `tree ${tree}\n${parentLines}\n${message}`;
      return enc.encode(body);
    }
    case "tag": {
      const {
        targetOid,
        targetType = "commit",
        tag = "v1",
        tagger = "You <you@example.com> 0 +0000",
        message = "",
      } = parts as TagParts;
      const body =
        `object ${targetOid}\n` +
        `type ${targetType}\n` +
        `tag ${tag}\n` +
        `tagger ${tagger}\n\n${message}`;
      return enc.encode(body);
    }
    default: {
      const { text = "" } = parts as BlobParts;
      return enc.encode(text);
    }
  }
}

// Inflate-and-parse round-trip for blob, commit, tree, tag
for (const type of ["blob", "commit", "tree", "tag"] as GitObjectType[]) {
  test(`inflateAndParseHeader parses ${type}`, async () => {
    const enc = new TextEncoder();
    let payload: Uint8Array;
    if (type === "blob") payload = enc.encode("hello");
    else if (type === "commit") {
      const tree = "a".repeat(40);
      const parents = ["b".repeat(40)];
      payload = textPayloadFor("commit", { tree, parents, message: "m" });
    } else if (type === "tree") {
      const oid1 = "b".repeat(40);
      const oid2 = "c".repeat(40);
      const e1Name = "file.txt";
      const e2Name = "dir";
      const parts: number[] = [];
      parts.push(...enc.encode("100644 "));
      parts.push(...enc.encode(e1Name));
      parts.push(0);
      parts.push(...hexToBytes(oid1));
      parts.push(...enc.encode("40000 "));
      parts.push(...enc.encode(e2Name));
      parts.push(0);
      parts.push(...hexToBytes(oid2));
      payload = new Uint8Array(parts);
    } else {
      // tag
      const targetOid = "d".repeat(40);
      payload = textPayloadFor("tag", { targetOid, targetType: "commit", message: "m" });
    }
    const { zdata } = await encodeGitObject(type, payload);
    const parsed = await inflateAndParseHeader(zdata);
    assert.ok(parsed);
    assert.strictEqual(parsed!.type, type);
    if (type === "commit") {
      const refs = parseCommitRefs(parsed!.payload);
      assert.match(refs.tree || "", /^[0-9a-f]{40}$/);
      assert.isTrue(Array.isArray(refs.parents));
    }
    if (type === "tree") {
      const kids = parseTreeChildOids(parsed!.payload);
      assert.isTrue(kids.length >= 2);
      assert.match(kids[0], /^[0-9a-f]{40}$/);
      assert.match(kids[1], /^[0-9a-f]{40}$/);
    }
    if (type === "tag") {
      const tag = parseTagTarget(parsed!.payload);
      assert.ok(tag);
      assert.match(tag!.targetOid, /^[0-9a-f]{40}$/);
      assert.strictEqual(typeof tag!.targetType, "string");
    }
  });
}

// Malformed zdata should return null
test("inflateAndParseHeader returns null on malformed data", async () => {
  const z = await deflate(new TextEncoder().encode("garbage-without-header"));
  const parsed = await inflateAndParseHeader(z);
  assert.strictEqual(parsed, null);
});
