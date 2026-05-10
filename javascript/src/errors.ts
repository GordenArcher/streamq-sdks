// Error classes for the streamq JavaScript SDK.
//
// Why classes instead of plain Error with a code property?
// TypeScript's instanceof narrowing works cleanly with class hierarchy:
//
//   try {
//     await client.createTopic("payments");
//   } catch (err) {
//     if (err instanceof StreamqConflict) {
//       // already exists, fine
//     } else if (err instanceof StreamqError) {
//       // other broker error
//     }
//   }
//
// All SDK errors extend StreamqError so callers can catch everything
// with a single catch block if they don't need granularity.

export class StreamqError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "StreamqError";
    // Fix prototype chain for instanceof checks in transpiled code.
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/** Broker returned 404. Typically: topic does not exist. */
export class StreamqNotFound extends StreamqError {
  constructor(message: string) {
    super(message);
    this.name = "StreamqNotFound";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/** Broker returned 409. Typically: topic already exists on createTopic(). */
export class StreamqConflict extends StreamqError {
  constructor(message: string) {
    super(message);
    this.name = "StreamqConflict";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/** Broker returned any other non-2xx status. */
export class StreamqAPIError extends StreamqError {
  constructor(
    public readonly statusCode: number,
    message: string,
  ) {
    super(`HTTP ${statusCode}: ${message}`);
    this.name = "StreamqAPIError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/** Consumer was explicitly closed via consumer.close(). */
export class StreamqConsumerClosed extends StreamqError {
  constructor() {
    super("consumer closed");
    this.name = "StreamqConsumerClosed";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/** Consumer exhausted its reconnect attempts after repeated failures. */
export class StreamqMaxReconnects extends StreamqError {
  constructor(
    public readonly attempts: number,
    public readonly lastError: unknown,
  ) {
    super(`gave up after ${attempts} reconnect attempt(s)`);
    this.name = "StreamqMaxReconnects";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
