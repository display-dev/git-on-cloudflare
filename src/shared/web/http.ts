/**
 * HTTP error class for consistent status propagation in UI routes.
 */
export class HttpError extends Error {
  status: number;
  expose: boolean;
  constructor(status: number, message: string, options?: { expose?: boolean; cause?: unknown }) {
    super(message);
    this.name = "HttpError";
    this.status = status;
    this.expose = Boolean(options?.expose);
    if (options?.cause) {
      // @ts-ignore
      this.cause = options.cause;
    }
  }
}

export function isHttpError(e: unknown): e is HttpError {
  return (
    e instanceof Error && (e as any).name === "HttpError" && typeof (e as any).status === "number"
  );
}
