import { describe, expect, it } from "vitest";

import { renderMarkdownToHtml } from "@/client/components/MarkdownContent";

const markdownContext = {
  owner: "alice",
  repo: "demo",
  ref: "main",
  baseDir: "docs/guide",
};

describe("renderMarkdownToHtml", () => {
  it("drops dangerous text-bearing elements and their children", () => {
    const html = renderMarkdownToHtml(
      [
        "<script>alert(1)</script>",
        "<style>.bad { color: red; }</style>",
        "<textarea>secret</textarea>",
        "<option>choice</option>",
        "<p>visible</p>",
      ].join(""),
      markdownContext
    );

    expect(html).toContain("<p>visible</p>");
    expect(html).not.toContain("alert");
    expect(html).not.toContain(".bad");
    expect(html).not.toContain("secret");
    expect(html).not.toContain("choice");
    expect(html).not.toContain("<script");
    expect(html).not.toContain("<style");
    expect(html).not.toContain("<textarea");
    expect(html).not.toContain("<option");
  });

  it("unwraps unknown tags while preserving sanitized children", () => {
    const html = renderMarkdownToHtml(
      "<unknown>before <strong>kept</strong><nested> after</nested></unknown>",
      markdownContext
    );

    expect(html).toContain("before <strong>kept</strong> after");
    expect(html).not.toContain("<unknown");
    expect(html).not.toContain("<nested");
  });

  it("strips event attributes from otherwise allowed elements", () => {
    const html = renderMarkdownToHtml(
      '<img src="/logo.png" alt="Logo" onerror="alert(1)" loading="lazy">',
      markdownContext
    );

    expect(html).toContain('<img src="/logo.png" alt="Logo" loading="lazy">');
    expect(html).not.toContain("onerror");
    expect(html).not.toContain("alert");
  });

  it("removes unsafe URL attributes", () => {
    const html = renderMarkdownToHtml(
      [
        '<a href="javascript:alert(1)">plain</a>',
        '<a href="java&#x73;cript&#58;alert(1)">encoded</a>',
        '<img src="data:text/html,alert(1)" alt="inline">',
      ].join(""),
      markdownContext
    );

    expect(html).toContain("<a>plain</a>");
    expect(html).toContain("<a>encoded</a>");
    expect(html).toContain('<img alt="inline">');
    expect(html).not.toContain("href=");
    expect(html).not.toContain("src=");
    expect(html).not.toContain("javascript");
    expect(html).not.toContain("data:text/html");
  });

  it("preserves allowed URL shapes and normalizes attribute escaping", () => {
    const html = renderMarkdownToHtml(
      [
        '<a href="http://example.com/a?x=1&y=2">http</a>',
        '<a href="https://example.com/a?x=1&amp;y=2">encoded</a>',
        '<a href="ftp://example.com/file">ftp</a>',
        '<a href="mailto:dev@example.com">mailto</a>',
        '<a href="tel:+15551234567">tel</a>',
        '<a href="//cdn.example.com/file">protocol</a>',
        '<a href="/root/path">root</a>',
        '<a href="../relative/path">relative</a>',
        '<a href="#section">fragment</a>',
      ].join(""),
      markdownContext
    );

    expect(html).toContain('href="http://example.com/a?x=1&amp;y=2"');
    expect(html).toContain('href="https://example.com/a?x=1&amp;y=2"');
    expect(html).not.toContain("&amp;amp;");
    expect(html).toContain('href="ftp://example.com/file"');
    expect(html).toContain('href="mailto:dev@example.com"');
    expect(html).toContain('href="tel:+15551234567"');
    expect(html).toContain('href="//cdn.example.com/file"');
    expect(html).toContain('href="/root/path"');
    expect(html).toContain('href="../relative/path"');
    expect(html).toContain('href="#section"');
  });

  it("filters classes to the current highlighting allowlist", () => {
    const html = renderMarkdownToHtml(
      [
        '<pre class="noise markdown-code-block">',
        '<code class="hljs language-ts extra">',
        '<span class="hljs-keyword extra">const</span>',
        "</code>",
        "</pre>",
      ].join(""),
      markdownContext
    );

    expect(html).toContain(
      '<pre class="markdown-code-block"><code class="hljs language-ts"><span class="hljs-keyword">const</span></code></pre>'
    );
    expect(html).not.toContain("noise");
    expect(html).not.toContain("extra");
  });

  it("rewrites relative Markdown links and images through repo routes", () => {
    const html = renderMarkdownToHtml(
      "[Doc](../README.md#intro)\n\n![Logo](assets/logo.png)",
      markdownContext
    );

    expect(html).toContain('href="/alice/demo/blob?ref=main&amp;path=docs%2FREADME.md#intro"');
    expect(html).toContain(
      'src="/alice/demo/rawpath?ref=main&amp;path=docs%2Fguide%2Fassets%2Flogo.png&amp;name=logo.png"'
    );
    expect(html).toContain('alt="Logo"');
    expect(html).toContain('loading="lazy"');
  });

  it("keeps Markdown after raw details blocks outside the disclosure", () => {
    const html = renderMarkdownToHtml(
      [
        "<details>",
        "<summary>Rationale</summary>",
        "Text before the list.",
        "",
        "1. first",
        "2. second",
        "</details>",
        "",
        "## After",
        "",
        "Visible outside.",
      ].join("\n"),
      markdownContext
    );
    const detailsCloseIndex = html.indexOf("</details>");
    const afterHeadingIndex = html.indexOf("<h2>After</h2>");

    expect(html).toContain("<ol>");
    expect(detailsCloseIndex).toBeGreaterThan(-1);
    expect(afterHeadingIndex).toBeGreaterThan(detailsCloseIndex);
    expect(html).toContain("<p>Visible outside.</p>");
  });

  it("preserves highlighted code block classes", () => {
    const html = renderMarkdownToHtml("```js\nconst answer = 1;\n```", markdownContext);

    expect(html).toContain('<pre class="markdown-code-block">');
    expect(html).toContain('<code class="hljs language-js">');
    expect(html).toContain('class="hljs-keyword"');
    expect(html).toContain("const");
  });
});
