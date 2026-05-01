// Syntax highlighting helpers for Highlight.js

/**
 * Infer Highlight.js language slug from a filename/path.
 * Returns null when unknown.
 */
export function inferHljsLang(fileName: string): string | null {
  const name = fileName || "";
  const lower = name.toLowerCase();
  // Special filenames
  if (lower === "makefile") return "makefile";
  if (lower === "dockerfile" || lower.endsWith("/dockerfile")) return "dockerfile";

  const lastDot = lower.lastIndexOf(".");
  const ext = lastDot >= 0 ? lower.slice(lastDot + 1) : "";

  switch (ext) {
    case "ts":
    case "tsx":
      return "typescript";
    case "js":
    case "mjs":
    case "cjs":
    case "jsx":
      return "javascript";
    case "json":
      return "json";
    case "go":
      return "go";
    case "py":
      return "python";
    case "rb":
      return "ruby";
    case "php":
      return "php";
    case "java":
      return "java";
    case "c":
      return "c";
    case "cc":
    case "cpp":
    case "cxx":
    case "hpp":
      return "cpp";
    case "cs":
      return "csharp";
    case "rs":
      return "rust";
    case "kt":
      return "kotlin";
    case "swift":
      return "swift";
    case "dart":
      return "dart";
    case "sql":
      return "sql";
    case "sh":
    case "bash":
    case "zsh":
      return "bash"; // highlight.js alias for sh
    case "ps1":
    case "psm1":
      return "powershell";
    case "less":
      return "less";
    case "lua":
      return "lua";
    case "yml":
    case "yaml":
      return "yaml";
    case "toml":
      return "toml";
    case "proto":
      return "protobuf";
    case "gradle":
      return "groovy";
    case "mm":
      return "objectivec";
    case "pl":
      return "perl";
    case "h":
      return "cpp";
    case "ini":
    case "cfg":
    case "conf":
      return "ini";
    case "md":
    case "markdown":
      return "markdown";
    case "html":
    case "htm":
    case "xml":
    case "svg":
      return "xml"; // highlight.js uses xml for HTML/XML/SVG
    case "css":
      return "css";
    case "scss":
    case "sass":
      return "scss";
    case "diff":
      return "diff";
    default:
      return null;
  }
}

/**
 * Default small set of languages useful for README/markdown pages.
 * Keep this list short to honor "load only when needed" while covering common fences.
 */
export function getMarkdownHighlightLangs(): string[] {
  return ["typescript", "javascript", "json", "bash", "yaml", "diff", "go", "python"];
}

/**
 * Infer language from a shebang line (e.g., #!/usr/bin/env python3)
 */
function inferFromShebang(text: string): string | null {
  const firstLine = (text || "").split(/\r?\n/, 1)[0];
  if (!firstLine || !firstLine.startsWith("#!")) return null;
  const lower = firstLine.toLowerCase();
  if (lower.includes("python")) return "python";
  if (lower.includes("node") || lower.includes("deno")) return "javascript";
  if (lower.includes("bash") || lower.includes("sh") || lower.includes("zsh")) return "bash";
  if (lower.includes("ruby")) return "ruby";
  if (lower.includes("ts-node")) return "typescript";
  return null;
}

/**
 * Smarter inference: use file extension first, then shebang fallback.
 */
export function inferHljsLangSmart(fileName: string, text?: string): string | null {
  const byExt = inferHljsLang(fileName);
  if (byExt) return byExt;
  if (text) {
    const byShebang = inferFromShebang(text);
    if (byShebang) return byShebang;
  }
  return null;
}

/**
 * Languages to load for a blob using smart inference.
 */
export function getHighlightLangsForBlobSmart(fileName: string, text?: string): string[] {
  const lang = inferHljsLangSmart(fileName, text);
  const normalized = normalizeLangForCdn(lang);
  return normalized ? [normalized] : [];
}

// Supported language slugs on our pinned CDN build. Keep this conservative.
const SUPPORTED_LANGS = new Set<string>([
  "typescript",
  "javascript",
  "json",
  "bash",
  "yaml",
  "diff",
  "go",
  "python",
  "ini",
  "xml",
  "css",
  "scss",
  "less",
  "java",
  "c",
  "cpp",
  "csharp",
  "rust",
  "kotlin",
  "swift",
  "php",
  "ruby",
  "sql",
  "powershell",
  "dockerfile",
  "makefile",
  "nginx",
  "objectivec",
  "perl",
  "protobuf",
  "groovy",
  "lua",
]);

/**
 * Normalize an inferred language slug to a CDN-available one.
 * - Map missing or alias languages to closest available grammar (e.g., toml -> ini)
 */
export function normalizeLangForCdn(lang: string | null): string | null {
  if (!lang) return null;
  let slug = lang;
  // Aliases or fallbacks
  if (slug === "toml") slug = "ini"; // close enough for highlighting
  // Ensure it's in our allowlist
  return SUPPORTED_LANGS.has(slug) ? slug : null;
}
