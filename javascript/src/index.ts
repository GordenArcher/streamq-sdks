// Public API surface for the streamq JavaScript SDK.
//
// Everything a caller needs is importable from "streamq-js":
//
//   import { StreamqClient, StreamqNotFound } from "streamq-js";
//   import type { Message, TopicInfo, ConsumerOptions } from "streamq-js";

export { StreamqClient } from "./client.js";
export type { ClientOptions } from "./client.js";

export { Consumer } from "./consumer.js";
export type { ConsumerOptions, Protocol } from "./consumer.js";

export { Message, TopicInfo, PublishResult } from "./models.js";

export {
  StreamqError,
  StreamqNotFound,
  StreamqConflict,
  StreamqAPIError,
  StreamqConsumerClosed,
  StreamqMaxReconnects,
} from "./errors.js";
