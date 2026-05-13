import { Marked } from "marked";

import { highlightCode } from "@/client/components/highlight";
import { sanitizeMarkdownHtml } from "@/client/components/markdownHtmlSanitizer";
import { escapeHtml } from "@/shared/web/format";

type MarkdownContext = {
  owner: string;
  repo: string;
  ref: string;
  baseDir?: string;
};

type MarkdownContentProps = {
  markdown: string;
  context: MarkdownContext;
};

function isAbsoluteUrl(href: string): boolean {
  return /^(?:[a-z]+:)?\/\//i.test(href) || href.startsWith("/") || href.startsWith("#");
}

function joinRelativePath(baseDir: string, relativePath: string): string {
  const [pathPart, hash = ""] = relativePath.split("#", 2);
  const baseParts = baseDir.split("/").filter(Boolean);

  for (const part of pathPart.split("/").filter(Boolean)) {
    if (part === ".") {
      continue;
    }
    if (part === "..") {
      baseParts.pop();
      continue;
    }
    baseParts.push(part);
  }

  const joined = baseParts.join("/");
  return hash ? `${joined}#${hash}` : joined;
}

function rewriteMarkdownHref(href: string, context: MarkdownContext): string {
  if (!href || isAbsoluteUrl(href)) {
    return href;
  }

  const hashIndex = href.indexOf("#");
  const fragment = hashIndex >= 0 ? href.slice(hashIndex) : "";
  const resolved = joinRelativePath(
    context.baseDir || "",
    hashIndex >= 0 ? href.slice(0, hashIndex) : href
  );

  return `/${encodeURIComponent(context.owner)}/${encodeURIComponent(context.repo)}/blob?ref=${encodeURIComponent(context.ref)}&path=${encodeURIComponent(resolved)}${fragment}`;
}

function rewriteMarkdownImage(href: string, context: MarkdownContext): string {
  if (!href || isAbsoluteUrl(href)) {
    return href;
  }

  const resolved = joinRelativePath(context.baseDir || "", href);
  const name = resolved.split("/").pop() || "file";
  return `/${encodeURIComponent(context.owner)}/${encodeURIComponent(context.repo)}/rawpath?ref=${encodeURIComponent(context.ref)}&path=${encodeURIComponent(resolved)}&name=${encodeURIComponent(name)}`;
}

function escapeAttribute(value: string): string {
  return escapeHtml(value).replace(/'/g, "&#39;");
}

function highlightBlock(code: string, language?: string | null): string {
  const highlighted = highlightCode(code, language);
  return `<pre class="markdown-code-block"><code class="hljs ${highlighted.languageClass}">${highlighted.html}</code></pre>`;
}

export function renderMarkdownToHtml(markdown: string, context: MarkdownContext): string {
  const marked = new Marked({ gfm: true, async: false });

  marked.use({
    renderer: {
      code({ text, lang }) {
        return highlightBlock(text, lang || null);
      },
      link({ href, title, tokens }) {
        const content = this.parser.parseInline(tokens);
        const resolvedHref = rewriteMarkdownHref(href || "", context);
        const titleAttr = title ? ` title="${escapeAttribute(title)}"` : "";
        return `<a href="${escapeAttribute(resolvedHref)}"${titleAttr}>${content}</a>`;
      },
      image({ href, title, text }) {
        const resolvedHref = rewriteMarkdownImage(href || "", context);
        const alt = escapeAttribute(text || "");
        const titleAttr = title ? ` title="${escapeAttribute(title)}"` : "";
        return `<img src="${escapeAttribute(resolvedHref)}" alt="${alt}"${titleAttr} loading="lazy" />`;
      },
    },
  });

  // async: false is set in the constructor, so parse() returns string synchronously.
  const raw = marked.parse(markdown);
  if (typeof raw !== "string") {
    throw new Error("marked returned an async render result despite async: false");
  }

  return sanitizeMarkdownHtml(raw);
}

export function MarkdownContent({ markdown, context }: MarkdownContentProps) {
  const html = renderMarkdownToHtml(markdown, context);
  return <div className="markdown-content" dangerouslySetInnerHTML={{ __html: html }} />;
}
