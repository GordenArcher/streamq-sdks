# Tests for model deserialisation — the base64 decoding, timestamp parsing,
# and from_dict() factories are where subtle bugs live.

import base64
from datetime import datetime, timezone

import pytest

from streamq.models import Message, PublishResult, TopicInfo


class TestMessage:
    def test_from_dict_decodes_base64_payload(self):
        raw = b'{"amount": 100}'
        encoded = base64.b64encode(raw).decode()

        msg = Message.from_dict(
            {
                "id": "42",
                "offset": 42,
                "topic": "payments",
                "payload": encoded,
                "timestamp": "2025-01-01T00:00:00Z",
            }
        )

        assert msg.payload == raw

    def test_from_dict_parses_timestamp_utc(self):
        msg = Message.from_dict(
            {
                "id": "0",
                "offset": 0,
                "topic": "t",
                "payload": "",
                "timestamp": "2025-06-01T12:00:00Z",
            }
        )

        assert msg.timestamp.year == 2025
        assert msg.timestamp.month == 6
        assert msg.timestamp.day == 1

    def test_from_dict_handles_empty_payload(self):
        msg = Message.from_dict(
            {
                "id": "0",
                "offset": 0,
                "topic": "t",
                "payload": "",
                "timestamp": "2025-01-01T00:00:00Z",
            }
        )
        assert msg.payload == b""

    def test_from_dict_handles_missing_fields_gracefully(self):
        # Partial data should not raise, use defaults.
        msg = Message.from_dict({})
        assert msg.id == ""
        assert msg.offset == 0
        assert msg.payload == b""

    def test_from_dict_handles_bytes_payload(self):
        # Some paths might give us bytes directly.
        msg = Message.from_dict(
            {
                "id": "1",
                "offset": 1,
                "topic": "t",
                "payload": b"hello",
                "timestamp": "2025-01-01T00:00:00Z",
            }
        )
        assert msg.payload == b"hello"


class TestTopicInfo:
    def test_from_dict_parses_all_fields(self):
        info = TopicInfo.from_dict(
            {
                "name": "payments",
                "message_count": 100,
                "subscriber_count": 3,
                "latest_offset": 99,
            }
        )

        assert info.name == "payments"
        assert info.message_count == 100
        assert info.subscriber_count == 3
        assert info.latest_offset == 99

    def test_from_dict_defaults_for_missing_fields(self):
        info = TopicInfo.from_dict({"name": "events"})
        assert info.message_count == 0
        assert info.latest_offset == -1


class TestPublishResult:
    def test_from_dict_parses_offset_and_topic(self):
        result = PublishResult.from_dict(
            {
                "offset": 7,
                "topic": "payments",
                "timestamp": "2025-01-01T00:00:00Z",
            }
        )

        assert result.offset == 7
        assert result.topic == "payments"
