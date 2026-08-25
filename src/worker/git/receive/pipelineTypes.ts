export type ReceivePipelineResult = {
  reportStatusBody: Uint8Array;
  changed: boolean;
  empty: boolean;
  packKey?: string;
  packBytes?: number;
  /** Byte-identical response emitted by stock receive-pack after durable commit. */
  receivePackResponse?: Uint8Array;
};

export class ReceivePipelineHttpError extends Error {
  readonly status: number;
  readonly reason: string;

  constructor(status: number, reason: string, message: string) {
    super(message);
    this.name = "ReceivePipelineHttpError";
    this.status = status;
    this.reason = reason;
  }
}

export class NativeReceiveIndeterminateError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "NativeReceiveIndeterminateError";
  }
}
