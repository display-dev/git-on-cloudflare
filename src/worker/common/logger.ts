export type LogLevel = "debug" | "info" | "warn" | "error";

const levelValue: Record<LogLevel, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

function parseLevel(input?: string | null): LogLevel {
  const v = (input || "").toLowerCase();
  if (v === "debug" || v === "info" || v === "warn" || v === "error") return v;
  return "info";
}

export interface LoggerContext {
  service: string;
  repoId?: string;
  doId?: string;
  requestId?: string;
}

export interface Logger {
  debug: (msg: string, extra?: Record<string, unknown>) => void;
  info: (msg: string, extra?: Record<string, unknown>) => void;
  warn: (msg: string, extra?: Record<string, unknown>) => void;
  error: (msg: string, extra?: Record<string, unknown>) => void;
}

function emit(
  level: LogLevel,
  ctx: LoggerContext,
  enabled: LogLevel,
  msg: string,
  extra?: Record<string, unknown>
) {
  if (levelValue[level] < levelValue[enabled]) return;
  const entry: Record<string, unknown> = {
    level,
    service: ctx.service,
  };
  if (ctx.repoId) entry.repoId = ctx.repoId;
  if (ctx.doId) entry.doId = ctx.doId;
  if (ctx.requestId) entry.requestId = ctx.requestId;
  entry.msg = msg;
  if (extra) {
    for (const [k, v] of Object.entries(extra)) entry[k] = v;
  }
  const line = JSON.stringify(entry);
  if (level === "debug") console.info(line);
  else if (level === "info") console.info(line);
  else if (level === "warn") console.info(line);
  else console.error(line);
}

export function createLogger(level: string | undefined, base: LoggerContext): Logger {
  const enabled = parseLevel(level);
  return {
    debug: (msg, extra) => emit("debug", base, enabled, msg, extra),
    info: (msg, extra) => emit("info", base, enabled, msg, extra),
    warn: (msg, extra) => emit("warn", base, enabled, msg, extra),
    error: (msg, extra) => emit("error", base, enabled, msg, extra),
  };
}
