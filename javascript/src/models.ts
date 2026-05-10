// Shared types for the streamq JavaScript SDK.
//
// Design: plain TypeScript interfaces + factory functions.
// We don't use classes for data types, plain objects are lighter,
// easier to serialise/log, and work naturally with destructuring.
//
// Payload handling:
// The broker sends payload as a base64 string inside JSON.
// We expose it as a Uint8Array (raw bytes) so callers can decode
// however they need:
//
//   const text = new TextDecoder().decode(msg.payload);
//   const parsed = JSON.parse(new TextDecoder().decode(msg.payload));

export interface Message {
  /** Broker-assigned unique identifier for this message. */
  id: string;

  /**
   * Zero-based absolute position in the topic log.
   * Use for ack() calls and fromOffset replay.
   */
  offset: number;

  /** Name of the topic this message was published to. */
  topic: string;

  /**
   * Raw message body as a Uint8Array.
   * The broker sends this as base64 in JSON, we decode automatically.
   * To get a string: new TextDecoder().decode(msg.payload)
   * To parse JSON:   JSON.parse(new TextDecoder().decode(msg.payload))
   */
  payload: Uint8Array;

  /** Server-side time the broker received the message. */
  timestamp: Date;
}

/**
 * Deserialise a broker JSON message object into a Message.
 * Handles base64 payload decoding and timestamp parsing.
 */
export function messageFromJSON(data: Record<string, unknown>): Message {
  // Payload arrives as a base64 string.
  // atob() decodes base64 → binary string; we convert to Uint8Array.
  const rawPayload = (data["payload"] as string) ?? "";
  let payload: Uint8Array;
  try {
    const binary = atob(rawPayload);
    payload = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
      payload[i] = binary.charCodeAt(i);
    }
  } catch {
    payload = new Uint8Array(0);
  }

  return {
    id: String(data["id"] ?? ""),
    offset: Number(data["offset"] ?? 0),
    topic: String(data["topic"] ?? ""),
    payload,
    timestamp: new Date(String(data["timestamp"] ?? "")),
  };
}

export interface TopicInfo {
  name: string;
  messageCount: number;
  subscriberCount: number;
  latestOffset: number;
}

export function topicInfoFromJSON(data: Record<string, unknown>): TopicInfo {
  return {
    name: String(data["name"] ?? ""),
    messageCount: Number(data["message_count"] ?? 0),
    subscriberCount: Number(data["subscriber_count"] ?? 0),
    latestOffset: Number(data["latest_offset"] ?? -1),
  };
}

export interface PublishResult {
  offset: number;
  topic: string;
  timestamp: Date;
}

export function publishResultFromJSON(
  data: Record<string, unknown>,
): PublishResult {
  return {
    offset: Number(data["offset"] ?? 0),
    topic: String(data["topic"] ?? ""),
    timestamp: new Date(String(data["timestamp"] ?? "")),
  };
}
