import { decodePktLines } from "@/worker/git/core";

/**
 * Parses Git fetch protocol v2 arguments from request body.
 * Extracts wants, haves, and done flag from pkt-line formatted data.
 *
 * @param body - Raw request body in pkt-line format
 * @returns Object containing wants array, haves array, and done flag
 */
export function parseFetchArgs(body: Uint8Array) {
  const items = decodePktLines(body);
  const wantSet = new Set<string>();
  const haves: string[] = [];
  let done = false;

  for (const item of items) {
    if (item.type === "line" && item.text) {
      const text = item.text.trimEnd();
      if (text.startsWith("want ")) {
        const oid = text.slice(5);
        if (oid.length >= 40) wantSet.add(oid.substring(0, 40));
      } else if (text.startsWith("have ")) {
        const oid = text.slice(5);
        if (oid.length >= 40) haves.push(oid.substring(0, 40));
      } else if (text === "done") {
        done = true;
      }
    }
  }

  const wants = Array.from(wantSet);
  return { wants, haves, done };
}
