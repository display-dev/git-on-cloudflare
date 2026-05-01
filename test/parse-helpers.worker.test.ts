import { it, expect, describe } from "vitest";
import { parseCommitRefs, parseTreeChildOids, inflateAndParseHeader } from "@/worker/git/core";
import { deflate } from "@/worker/common";

describe("shared parse helpers", () => {
  it("parseCommitRefs extracts tree and parents correctly", () => {
    const commitContent = new TextEncoder().encode(
      `tree 4b825dc642cb6eb9a060e54bf8d69288fbee4904
parent 1234567890abcdef1234567890abcdef12345678
parent abcdef1234567890abcdef1234567890abcdef12
author Test User <test@example.com> 1234567890 +0000
committer Test User <test@example.com> 1234567890 +0000

Test commit message`
    );

    const refs = parseCommitRefs(commitContent);
    expect(refs.tree).toBe("4b825dc642cb6eb9a060e54bf8d69288fbee4904");
    expect(refs.parents).toEqual([
      "1234567890abcdef1234567890abcdef12345678",
      "abcdef1234567890abcdef1234567890abcdef12",
    ]);
  });

  it("parseTreeChildOids extracts child OIDs from tree content", () => {
    // Tree entry format: mode<space>name<null>20-byte-sha
    const entries: Uint8Array[] = [];

    // Add a file entry
    entries.push(new TextEncoder().encode("100644 file.txt"));
    entries.push(new Uint8Array([0])); // null terminator
    entries.push(
      new Uint8Array([
        0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd,
        0xef, 0x12, 0x34, 0x56, 0x78,
      ])
    );

    // Add a directory entry
    entries.push(new TextEncoder().encode("40000 dir"));
    entries.push(new Uint8Array([0])); // null terminator
    entries.push(
      new Uint8Array([
        0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78,
        0x90, 0xab, 0xcd, 0xef, 0x12,
      ])
    );

    // Concatenate all entries
    let totalLength = 0;
    for (const entry of entries) {
      totalLength += entry.length;
    }
    const treeContent = new Uint8Array(totalLength);
    let offset = 0;
    for (const entry of entries) {
      treeContent.set(entry, offset);
      offset += entry.length;
    }

    const childOids = parseTreeChildOids(treeContent);
    expect(childOids).toEqual([
      "1234567890abcdef1234567890abcdef12345678",
      "abcdef1234567890abcdef1234567890abcdef12",
    ]);
  });

  it("inflateAndParseHeader correctly parses compressed git objects", async () => {
    const payload = new TextEncoder().encode("Hello, World!");
    const header = new TextEncoder().encode(`blob ${payload.length}\0`);
    const raw = new Uint8Array(header.length + payload.length);
    raw.set(header, 0);
    raw.set(payload, header.length);

    const compressed = await deflate(raw);

    const result = await inflateAndParseHeader(compressed);
    expect(result).toBeDefined();
    expect(result?.type).toBe("blob");
    expect(new TextDecoder().decode(result?.payload)).toBe("Hello, World!");
  });
});
