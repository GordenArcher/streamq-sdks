// Shared data types for the streamq Rust SDK.
//
// Design: serde-derived structs for zero-boilerplate JSON deserialisation.
// Rust's serde ecosystem is the gold standard for this, we derive
// Deserialize on every type that comes from the broker, and Serialize
// on types we send to it.
//
// Payload handling:
// The broker sends payload as a base64 string inside JSON. We define a
// custom deserialiser that decodes base64 → Vec<u8> automatically so
// callers always receive raw bytes.
//
// Why Vec<u8> instead of Bytes?
// Vec<u8> is the standard owned byte buffer in Rust's stdlib. Callers
// who need zero-copy access can convert to Bytes cheaply. Keeping the
// type as Vec<u8> avoids pulling in the `bytes` crate as a dependency.

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};

/// A single event received from a streamq topic.
#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    /// Broker-assigned unique identifier for this message.
    pub id: String,

    /// Zero-based absolute position in the topic log.
    /// Use for `ack()` calls and `from_offset` replay.
    pub offset: i64,

    /// Name of the topic this message was published to.
    pub topic: String,

    /// Raw message body as bytes.
    /// Decoded from base64 automatically, callers receive raw bytes.
    pub payload: Vec<u8>,

    /// Server-side time the broker received this message.
    pub timestamp: DateTime<Utc>,
}

// Manual Deserialize implementation because we need custom base64 decoding
// for the payload field. The rest can use standard serde field mapping.
impl<'de> Deserialize<'de> for Message {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Intermediate struct that matches the broker's JSON shape exactly.
        #[derive(Deserialize)]
        struct Raw {
            id: String,
            offset: i64,
            topic: String,
            payload: String, // base64 string from broker
            timestamp: DateTime<Utc>,
        }

        let raw = Raw::deserialize(deserializer)?;

        // Decode base64 payload. On failure, use empty bytes rather than
        // propagating an error, a missing or empty payload is still a valid
        // message; we don't want to fail deserialisation for it.
        let payload = BASE64.decode(&raw.payload).unwrap_or_default();

        Ok(Message {
            id: raw.id,
            offset: raw.offset,
            topic: raw.topic,
            payload,
            timestamp: raw.timestamp,
        })
    }
}

/// Stats for a single topic, returned by `list_topics()` and `get_topic()`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TopicInfo {
    pub name: String,
    pub message_count: i64,
    pub subscriber_count: i64,
    pub latest_offset: i64,
}

/// Broker response to a publish request.
#[derive(Debug, Clone, Deserialize)]
pub struct PublishResult {
    pub offset: i64,
    pub topic: String,
    pub timestamp: DateTime<Utc>,
}

/// The broker wraps all responses in a standard JSON envelope:
/// `{"status": "success"|"error", "data": ..., "message": "..."}`
///
/// This internal type is used by the HTTP client to unwrap responses.
/// Not part of the public API.
#[derive(Debug, Deserialize)]
pub(crate) struct BrokerEnvelope<T> {
    pub status: String,
    pub data: Option<T>,
    pub message: Option<String>,
}

/// Broker error response envelope.
#[derive(Debug, Deserialize)]
pub(crate) struct BrokerError {
    pub message: Option<String>,
}

/// Sent by the consumer to the broker over WebSocket to acknowledge a message.
/// Only used for grouped WebSocket consumers.
#[derive(Debug, Serialize)]
pub(crate) struct AckFrame {
    pub ack: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_deserialises_base64_payload() {
        // "hello" base64-encodes to "aGVsbG8="
        let json = r#"{
            "id": "1",
            "offset": 1,
            "topic": "payments",
            "payload": "aGVsbG8=",
            "timestamp": "2025-01-01T00:00:00Z"
        }"#;

        let msg: Message = serde_json::from_str(json).unwrap();
        assert_eq!(msg.payload, b"hello");
        assert_eq!(msg.offset, 1);
        assert_eq!(msg.topic, "payments");
    }

    #[test]
    fn message_handles_empty_payload() {
        let json = r#"{
            "id": "0",
            "offset": 0,
            "topic": "t",
            "payload": "",
            "timestamp": "2025-01-01T00:00:00Z"
        }"#;

        let msg: Message = serde_json::from_str(json).unwrap();
        assert!(msg.payload.is_empty());
    }

    #[test]
    fn topic_info_deserialises() {
        let json = r#"{
            "name": "payments",
            "message_count": 42,
            "subscriber_count": 3,
            "latest_offset": 41
        }"#;

        let info: TopicInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.name, "payments");
        assert_eq!(info.message_count, 42);
        assert_eq!(info.latest_offset, 41);
    }
}
