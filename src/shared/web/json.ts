export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonObject | JsonValue[];
export type JsonObject = { [key: string]: JsonValue };

export function isJsonObject(value: JsonValue | null | undefined): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export async function safeParseJsonRequest(request: Request): Promise<JsonValue | null> {
  try {
    const text = await request.text();
    if (!text) return null;
    return JSON.parse(text) as JsonValue;
  } catch {
    return null;
  }
}
