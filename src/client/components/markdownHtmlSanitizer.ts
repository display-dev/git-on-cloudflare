import { hasChildren, isComment, isDirective, isDocument, isTag, isText } from "domhandler";
import type { ChildNode, Element, ParentNode } from "domhandler";
import { parseDocument } from "htmlparser2";

// This list intentionally mirrors sanitize-html 2.17.1 defaults. Keeping the
// policy local lets the Worker avoid sanitize-html's Node compatibility needs
// without quietly widening the Markdown raw-HTML surface.
const DEFAULT_ALLOWED_TAGS = [
  "address",
  "article",
  "aside",
  "footer",
  "header",
  "h1",
  "h2",
  "h3",
  "h4",
  "h5",
  "h6",
  "hgroup",
  "main",
  "nav",
  "section",
  "blockquote",
  "dd",
  "div",
  "dl",
  "dt",
  "figcaption",
  "figure",
  "hr",
  "li",
  "menu",
  "ol",
  "p",
  "pre",
  "ul",
  "a",
  "abbr",
  "b",
  "bdi",
  "bdo",
  "br",
  "cite",
  "code",
  "data",
  "dfn",
  "em",
  "i",
  "kbd",
  "mark",
  "q",
  "rb",
  "rp",
  "rt",
  "rtc",
  "ruby",
  "s",
  "samp",
  "small",
  "span",
  "strong",
  "sub",
  "sup",
  "time",
  "u",
  "var",
  "wbr",
  "caption",
  "col",
  "colgroup",
  "table",
  "tbody",
  "td",
  "tfoot",
  "th",
  "thead",
  "tr",
] satisfies string[];

const REPO_ALLOWED_TAGS = ["img", "details", "summary", "del", "ins"] satisfies string[];
const ALLOWED_TAGS = new Set<string>([...DEFAULT_ALLOWED_TAGS, ...REPO_ALLOWED_TAGS]);

const DROP_WITH_CHILDREN_TAGS = new Set<string>(["script", "style", "textarea", "option"]);
const ALLOWED_URL_SCHEMES = new Set<string>(["http", "https", "ftp", "mailto", "tel"]);
const ALLOWED_EMPTY_ATTRIBUTES = new Set<string>(["alt"]);
const VOID_TAGS = new Set<string>([
  "area",
  "base",
  "basefont",
  "br",
  "col",
  "embed",
  "hr",
  "img",
  "input",
  "keygen",
  "link",
  "meta",
  "param",
  "source",
  "track",
  "wbr",
]);

const ALLOWED_ATTRIBUTES_BY_TAG = new Map<string, ReadonlySet<string>>([
  ["a", new Set(["href", "title", "name", "target", "rel"])],
  ["img", new Set(["src", "alt", "title", "loading"])],
  ["code", new Set(["class"])],
  ["span", new Set(["class"])],
  ["pre", new Set(["class"])],
  ["td", new Set(["align"])],
  ["th", new Set(["align"])],
]);

const ATTRIBUTE_AMPERSAND_RE =
  /&(?!(?:#[0-9]{1,7}|#x[0-9a-fA-F]{1,6}|[a-zA-Z][a-zA-Z0-9]{1,31});)/g;
const HTML_CHARACTER_REFERENCE_RE =
  /&(?:#([0-9]{1,7})|#x([0-9a-fA-F]{1,6})|([a-zA-Z][a-zA-Z0-9]{1,31}));?/g;
const URL_CONTROL_AND_SPACE_RE = /[\u0000-\u0020]+/g;
const URL_SCHEME_RE = /^([a-zA-Z][a-zA-Z0-9.+-]*):/;

const URL_NAMED_CHARACTER_REFERENCES = new Map<string, string>([
  ["amp", "&"],
  ["apos", "'"],
  ["bsol", "\\"],
  ["colon", ":"],
  ["gt", ">"],
  ["lt", "<"],
  ["NewLine", "\n"],
  ["quot", '"'],
  ["sol", "/"],
  ["Tab", "\t"],
]);

function escapeTextValue(value: string): string {
  return value.replace(/[&<>]/g, (character) => {
    if (character === "&") {
      return "&amp;";
    }
    if (character === "<") {
      return "&lt;";
    }
    return "&gt;";
  });
}

function escapeAttributeValue(value: string): string {
  // htmlparser2 decodes character references before this point, so every
  // attribute value is serialized from a normalized string. That prevents
  // double-encoding `?a=1&amp;b=2` while still making quotes and brackets inert.
  return value
    .replace(ATTRIBUTE_AMPERSAND_RE, "&amp;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function decodeHtmlCharacterReference(
  entity: string,
  decimal: string | undefined,
  hexadecimal: string | undefined,
  named: string | undefined
): string {
  if (decimal) {
    return decodeCodePoint(entity, decimal, 10);
  }

  if (hexadecimal) {
    return decodeCodePoint(entity, hexadecimal, 16);
  }

  if (named) {
    return URL_NAMED_CHARACTER_REFERENCES.get(named) ?? entity;
  }

  return entity;
}

function decodeCodePoint(entity: string, rawCodePoint: string, radix: number): string {
  const codePoint = Number.parseInt(rawCodePoint, radix);
  if (!Number.isFinite(codePoint) || codePoint < 0 || codePoint > 0x10ffff) {
    return entity;
  }

  try {
    return String.fromCodePoint(codePoint);
  } catch {
    return entity;
  }
}

function decodeHtmlCharacterReferences(value: string): string {
  return value.replace(HTML_CHARACTER_REFERENCE_RE, decodeHtmlCharacterReference);
}

function removeHtmlCommentsFromUrl(value: string): string {
  let normalized = value;

  while (true) {
    const start = normalized.indexOf("<!--");
    if (start < 0) {
      return normalized;
    }

    const end = normalized.indexOf("-->", start + 4);
    if (end < 0) {
      return normalized;
    }

    normalized = `${normalized.slice(0, start)}${normalized.slice(end + 3)}`;
  }
}

function isAllowedUrl(value: string): boolean {
  // Match sanitize-html's URL-scheme stance: strip characters browsers ignore
  // in URL protocols, decode references that can hide `javascript:`, then only
  // reject values with an explicit non-allowed scheme. Relative paths, root
  // paths, fragments, and protocol-relative URLs therefore remain valid.
  const decoded = decodeHtmlCharacterReferences(value);
  const withoutComments = removeHtmlCommentsFromUrl(decoded);
  const normalized = withoutComments.replace(URL_CONTROL_AND_SPACE_RE, "");
  const schemeMatch = URL_SCHEME_RE.exec(normalized);
  if (!schemeMatch) {
    return true;
  }

  return ALLOWED_URL_SCHEMES.has(schemeMatch[1].toLowerCase());
}

function isUrlAttribute(attributeName: string): boolean {
  return attributeName === "href" || attributeName === "src";
}

function isAllowedClassName(elementName: string, className: string): boolean {
  if (elementName === "code") {
    return className === "hljs" || className.startsWith("language-");
  }

  if (elementName === "span") {
    return className.startsWith("hljs-");
  }

  if (elementName === "pre") {
    return className === "markdown-code-block";
  }

  return false;
}

function filterClassAttribute(elementName: string, value: string): string {
  return value
    .split(/\s+/)
    .filter((className) => className.length > 0 && isAllowedClassName(elementName, className))
    .join(" ");
}

function sanitizeAttributes(node: Element, elementName: string): void {
  const allowedAttributes = ALLOWED_ATTRIBUTES_BY_TAG.get(elementName);
  if (!allowedAttributes) {
    node.attribs = {};
    return;
  }

  const sanitizedAttributes: Record<string, string> = {};
  for (const [rawAttributeName, rawValue] of Object.entries(node.attribs)) {
    const attributeName = rawAttributeName.toLowerCase();
    if (!allowedAttributes.has(attributeName)) {
      continue;
    }

    const filteredValue =
      attributeName === "class" ? filterClassAttribute(elementName, rawValue) : rawValue;
    if (filteredValue.length === 0 && !ALLOWED_EMPTY_ATTRIBUTES.has(attributeName)) {
      continue;
    }

    if (isUrlAttribute(attributeName) && !isAllowedUrl(filteredValue)) {
      continue;
    }

    sanitizedAttributes[attributeName] = filteredValue;
  }

  node.attribs = sanitizedAttributes;
}

function sanitizeChildren(parent: ParentNode): void {
  const sanitizedChildren: ChildNode[] = [];
  for (const child of parent.children) {
    for (const sanitizedChild of sanitizeNode(child)) {
      sanitizedChildren.push(sanitizedChild);
    }
  }

  relinkChildren(parent, sanitizedChildren);
}

function sanitizeNode(node: ChildNode): ChildNode[] {
  if (isText(node)) {
    return [node];
  }

  if (isComment(node) || isDirective(node) || isDocument(node)) {
    return [];
  }

  if (!isTag(node)) {
    if (hasChildren(node)) {
      sanitizeChildren(node);
      return node.children;
    }
    return [];
  }

  const elementName = node.name.toLowerCase();
  if (DROP_WITH_CHILDREN_TAGS.has(elementName)) {
    return [];
  }

  node.name = elementName;
  sanitizeChildren(node);

  if (!ALLOWED_TAGS.has(elementName)) {
    return node.children;
  }

  sanitizeAttributes(node, elementName);
  return [node];
}

function relinkChildren(parent: ParentNode, children: ChildNode[]): void {
  parent.children = children;

  children.forEach((child, index) => {
    child.parent = parent;
    child.prev = children[index - 1] ?? null;
    child.next = children[index + 1] ?? null;
  });
}

function serializeAttributes(attributes: Record<string, string>): string {
  let serialized = "";
  for (const [name, value] of Object.entries(attributes)) {
    if (value.length > 0 || ALLOWED_EMPTY_ATTRIBUTES.has(name)) {
      serialized += ` ${name}="${escapeAttributeValue(value)}"`;
    } else {
      serialized += ` ${name}`;
    }
  }
  return serialized;
}

function serializeNode(node: ChildNode): string {
  if (isText(node)) {
    return escapeTextValue(node.data);
  }

  if (!isTag(node)) {
    return "";
  }

  const elementName = node.name.toLowerCase();
  const attributes = serializeAttributes(node.attribs);
  if (VOID_TAGS.has(elementName)) {
    return `<${elementName}${attributes}>`;
  }

  return `<${elementName}${attributes}>${serializeChildren(node.children)}</${elementName}>`;
}

function serializeChildren(children: readonly ChildNode[]): string {
  return children.map((child) => serializeNode(child)).join("");
}

export function sanitizeMarkdownHtml(rawHtml: string): string {
  const document = parseDocument(rawHtml, { decodeEntities: true });
  sanitizeChildren(document);
  return serializeChildren(document.children);
}
