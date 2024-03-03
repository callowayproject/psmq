"""Tests of the Queue class."""

import pytest
from redis.client import Redis

from psmq import queue_ops
from psmq.exceptions import NoMessageInQueue, UndeserializableMessage, UnserializableMessage
from psmq.queue import Queue, QueueConfiguration, QueueMetadata
from psmq.serialize import default_deserializer, default_serializer


class TestCreation:
    """Tests the creation of a Queue object."""

    def test_instantiation_creates_queue(self, conn: Redis):
        """Creating a Queue object will create the queue in Redis."""
        q = Queue(conn, "test_queue")
        assert q.name == "test_queue"
        assert q._configuration == QueueConfiguration()
        assert isinstance(q._metadata, QueueMetadata)
        assert q.serializer is default_serializer
        assert q.deserializer is default_deserializer
        assert "test_queue" in queue_ops.list_queues(conn)

    def test_instantiation_is_idempotent(self, conn: Redis):
        """Creating a Queue object is idempotent."""
        q1 = Queue(conn, "test_queue")
        assert q1.name == "test_queue"
        assert "test_queue" in queue_ops.list_queues(conn)
        q2 = Queue(conn, "test_queue")
        assert q2.name == "test_queue"


class TestSerialization:
    """Tests the serialization of messages."""

    def test_calls_serializer_function(self, conn: Redis, mocker):
        """You can serialize a message."""
        mocked_serializer = mocker.Mock()
        mocked_serializer.return_value = b"test"
        q = Queue(conn, "test_queue", serializer=mocked_serializer)
        assert q.serialize("test") == b"test"
        assert mocked_serializer.call_count == 1
        mocked_serializer.assert_called_with("test")

    def test_wraps_errors(self, conn: Redis, mocker):
        """Attempting to serialize a message with a serializer that raises an exception raises an exception."""
        mocked_serializer = mocker.Mock(__name__="test_serializer")
        mocked_serializer.side_effect = Exception("test")
        q = Queue(conn, "test_queue", serializer=mocked_serializer)
        with pytest.raises(UnserializableMessage):
            q.serialize("test")


class TestDeserialization:
    """Tests the deserialization of messages."""

    def test_calls_deserializer(self, conn: Redis, mocker):
        """You can deserialize a message."""
        mocked_deserializer = mocker.Mock()
        mocked_deserializer.return_value = "test"
        q = Queue(conn, "test_queue", deserializer=mocked_deserializer)
        assert q.deserialize(b"test") == "test"
        assert mocked_deserializer.call_count == 1
        mocked_deserializer.assert_called_with(b"test")

    def test_deserialize_wraps_errors(self, conn: Redis, mocker):
        """Attempting to deserialize a message with a deserializer that raises an exception raises an exception."""
        mocked_deserializer = mocker.Mock(__name__="test_deserializer")
        mocked_deserializer.side_effect = Exception("test")
        q = Queue(conn, "test_queue", deserializer=mocked_deserializer)
        with pytest.raises(UndeserializableMessage):
            q.deserialize(b"test")


class TestGetMessage:
    def test_leaves_message_on_queue(self, conn: Redis):
        """Test get method leaves the message on the queue."""
        q = Queue(conn, "test_queue")
        msg_id = q.push("test")
        assert msg_id is not None
        assert q.metadata().msgs == 1
        msg = q.get()
        assert msg.message_id == msg_id
        assert msg.data == "test"
        assert q.metadata().msgs == 1

    def test_on_empty_queue_returns_none(self, conn: Redis):
        """Test get method on an empty queue returns None."""
        q = Queue(conn, "test_queue")
        assert q.get() is None

    def test_with_raise_on_empty_raises_error(self, conn: Redis):
        """Test get method with raise_on_empty=True raises an error."""
        q = Queue(conn, "test_queue")
        with pytest.raises(NoMessageInQueue):
            q.get(raise_on_empty=True)


class TestDeleteMessage:
    def test_deletes_message_from_queue(self, conn: Redis):
        """Test delete method deletes the message from the queue."""
        q = Queue(conn, "test_queue")
        msg_id = q.push("test")
        assert msg_id is not None
        assert q.metadata().msgs == 1
        msg_id2 = q.push("test2")
        assert msg_id2 is not None
        assert q.metadata().msgs == 2
        q.delete(msg_id)
        assert q.metadata().msgs == 1

    def test_is_idempotent(self, conn: Redis):
        """Test delete method is idempotent."""
        q = Queue(conn, "test_queue")
        msg_id = q.push("test")
        assert msg_id is not None
        assert q.metadata().msgs == 1
        q.delete(msg_id)
        assert q.metadata().msgs == 0
        q.delete(msg_id)
        assert q.metadata().msgs == 0


def test_push(conn: Redis):
    """Test push method."""
    q = Queue(conn, "test_queue")
    msg_id = q.push("test")
    assert msg_id is not None
    assert q.metadata().msgs == 1


def test_push_many(conn: Redis):
    """Test push_many method."""
    q = Queue(conn, "test_queue")
    msg_ids = q.push_many(["test", "test2"])
    assert len(msg_ids) == 2
    assert q.metadata().msgs == 2
    assert all(isinstance(msg_id, str) for msg_id in msg_ids)


def test_pop_returns_message_and_deletes_it(conn: Redis):
    """Test pop method."""
    q = Queue(conn, "test_queue")
    msg_id = q.push("test")
    assert msg_id is not None
    assert q.metadata().msgs == 1
    msg = q.pop()
    assert msg.message_id == msg_id
    assert msg.data == "test"
    assert q.metadata().msgs == 0


def test_pop_on_empty_queue_returns_none(conn: Redis):
    """Test pop method on an empty queue returns None."""
    q = Queue(conn, "test_queue")
    assert q.pop() is None
