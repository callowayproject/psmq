"""Tests for streams."""

import pytest

from psmq import stream
from redis.client import Redis


class TestStreamMessage:
    """Make sure StreamMessage works as expected."""

    def test_returns_a_value_from_attribute(self):
        """Test getting attributes from a StreamMessage."""
        message = stream.StreamMessage("0-0", ["key", "value"])
        assert message.message_id == "0-0"
        assert message.key == "value"

    def test_raises_attribute_error_for_missing_key(self):
        """Test getting an attribute that doesn't exist."""
        message = stream.StreamMessage("0-0", ["key", "value"])
        with pytest.raises(AttributeError):
            _ = message.nonexistent_key


class TestStream:
    def test_create_consumer_group(self, conn: Redis):
        """Test creating a consumer group."""
        stream_instance = stream.Stream(conn, "test")
        stream_instance.create_consumer_group("test_group")
        assert conn.xinfo_groups("test") == [
            {
                "name": b"test_group",
                "consumers": 0,
                "pending": 0,
                "last-delivered-id": b"0-0",
                "entries-read": None,
                "lag": 0,
            }
        ]

    def test_create_consumer(self, conn: Redis):
        """Test creating a consumer."""
        stream_instance = stream.Stream(conn, "test")
        stream_instance.create_consumer("test_group", "test_consumer")
        assert conn.xinfo_consumers("test", "test_group") == [
            {"name": b"test_consumer", "pending": 0, "idle": 0, "inactive": -1}
        ]
        # Check for idempotency
        stream_instance.create_consumer("test_group", "test_consumer")
        consumer_names = [x["name"] for x in conn.xinfo_consumers("test", "test_group")]
        assert consumer_names == [b"test_consumer"]


class TestConsumer:
    """Basic consumer tests."""

    def test_get(self, conn: Redis):
        """Test getting messages from the stream."""
        consumer = stream.Consumer(conn, "test", "test_group", "test_consumer")
        conn.xadd("test", {"key": "value"})
        messages = consumer.get()
        assert len(messages) == 1
        assert messages[0].key == "value"

    def test_ack(self, conn: Redis):
        """Test acknowledging messages."""
        consumer = stream.Consumer(conn, "test", "test_group", "test_consumer")
        conn.xadd("test", {"key": "value"})
        messages = consumer.get()
        consumer.ack(messages[0].message_id)
        messages = consumer.get()
        assert len(messages) == 0

    def test_pop(self, conn: Redis):
        """Test popping messages from the stream."""
        consumer = stream.Consumer(conn, "test", "test_group", "test_consumer")
        conn.xadd("test", {"key": "value"})
        messages = consumer.pop()
        assert len(messages) == 1
        assert messages[0].key == "value"
        messages = consumer.get()
        assert len(messages) == 0
