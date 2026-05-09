# Shared data types for the streamq Python SDK.
#
# Design: plain dataclasses (not Pydantic) to keep dependencies minimal.
# Python's dataclass gives us __repr__, __eq__, and type hints for free
# without pulling in a validation library. Callers who want Pydantic can
# wrap these types trivially.
#
# Payload handling:
# The broker sends payload as a base64-encoded string inside JSON.
# We decode it to bytes here so callers always work with raw bytes,
# regardless of what encoding the producer used. Callers decode further
# (json.loads, protobuf.ParseFromString, etc.) as needed.

from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class Message:
    """A single event received from a streamq topic."""

    # Broker-assigned unique identifier for this message.
    id: str

    # Zero-based absolute position in the topic log.
    # Use for Ack() calls and WithFromOffset() replay.
    offset: int

    # Name of the topic this message was published to.
    topic: str

    # Raw message body as bytes.
    # The broker sends this as base64 in JSON, we decode automatically.
    payload: bytes

    # Server-side time the broker received the message.
    timestamp: datetime

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Message":
        """Deserialise a broker JSON message dict into a Message.

        Handles base64 payload decoding automatically.
        """
        # Payload arrives as a base64 string in JSON.
        # json.loads gives us a str; base64.b64decode gives us bytes.
        raw_payload = data.get("payload", "")
        if isinstance(raw_payload, str):
            payload = base64.b64decode(raw_payload)
        elif isinstance(raw_payload, bytes):
            payload = raw_payload
        else:
            payload = b""

        # Parse ISO 8601 timestamp. Python 3.11+ supports fromisoformat
        # for the full RFC 3339 format; for 3.9/3.10 compatibility we
        # strip the trailing 'Z' and replace with '+00:00'.
        ts_str = data.get("timestamp", "")
        if ts_str.endswith("Z"):
            ts_str = ts_str[:-1] + "+00:00"
        try:
            timestamp = datetime.fromisoformat(ts_str)
        except (ValueError, AttributeError):
            timestamp = datetime.utcnow()

        return cls(
            id=data.get("id", ""),
            offset=data.get("offset", 0),
            topic=data.get("topic", ""),
            payload=payload,
            timestamp=timestamp,
        )


@dataclass
class TopicInfo:
    """Stats for a single topic returned by list_topics() and get_topic()."""

    name: str
    message_count: int
    subscriber_count: int
    latest_offset: int

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TopicInfo":
        return cls(
            name=data.get("name", ""),
            message_count=data.get("message_count", 0),
            subscriber_count=data.get("subscriber_count", 0),
            latest_offset=data.get("latest_offset", -1),
        )


@dataclass
class PublishResult:
    """Broker response to a publish request."""

    offset: int
    topic: str
    timestamp: datetime

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PublishResult":
        ts_str = data.get("timestamp", "")
        if ts_str.endswith("Z"):
            ts_str = ts_str[:-1] + "+00:00"
        try:
            timestamp = datetime.fromisoformat(ts_str)
        except (ValueError, AttributeError):
            timestamp = datetime.utcnow()

        return cls(
            offset=data.get("offset", 0),
            topic=data.get("topic", ""),
            timestamp=timestamp,
        )
