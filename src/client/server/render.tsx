import { renderToReadableStream } from "react-dom/server";

import { AppLayout } from "@/client/components/AppLayout";
import { resolveDocumentAssets } from "./assets";
import { Document } from "./document";
import { getViewDefinition } from "./registry";
import type { Viewer } from "./viewer";

function needsHighlightTheme(name: string, data: Record<string, unknown>): boolean {
  return (
    name === "blob" ||
    name === "commit" ||
    (name === "overview" && typeof data.readmeMd === "string" && data.readmeMd.length > 0)
  );
}

export type RenderUiViewOptions = {
  // The signed-in viewer (if any) is computed by the Worker route handler
  // and threaded through the SSR shell. The renderer stays presentational
  // and never touches cookies or D1 itself.
  viewer?: Viewer | null;
};

export async function renderUiView(
  env: Env,
  name: string,
  data: Record<string, unknown>,
  options: RenderUiViewOptions = {}
): Promise<BodyInit | null> {
  const definition = getViewDefinition(name);
  if (!definition) {
    return null;
  }

  const page = definition.render(data);
  if (definition.kind === "fragment") {
    // Fragments don't need a full document wrapper — React won't emit a doctype
    return renderToReadableStream(page);
  }

  const assets = await resolveDocumentAssets(env, definition.clientEntrypoints || []);
  const element = (
    <Document
      title={(data.title as string | undefined) || definition.title}
      assets={assets}
      needsHighlight={needsHighlightTheme(name, data)}
    >
      <AppLayout currentView={name} viewer={options.viewer ?? null}>
        {page}
      </AppLayout>
    </Document>
  );

  // React's renderToReadableStream automatically emits <!DOCTYPE html>
  // when the root element is <html>
  return renderToReadableStream(element);
}
