import type { JsonValue } from "@/shared/web";

export async function safeReadJson(response: Response): Promise<JsonValue | null> {
  try {
    return (await response.json()) as JsonValue;
  } catch {
    return null;
  }
}
