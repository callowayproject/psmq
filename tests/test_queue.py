"""Tests of the Queue class."""
import pytest

from psmq.connection import RedisLiteConnection
from psmq.exceptions import UnserializableMessage, UndeserializableMessage, NoMessageInQueue
from psmq.queue import Queue, QueueConfiguration, QueueMetadata


class TestQueue:
    """Tests of the Queue class."""

    class TestCreation:
        """Tests the creation of a Queue object."""

        def test_instantiation(self, conn: RedisLiteConnection):
            """You can instantiate a Queue object with only a connection and name."""
            q = Queue(conn, "test_queue")
            assert q.name == "test_queue"
            assert q._configuration == QueueConfiguration()
            assert isinstance(q._metadata, QueueMetadata)
            assert q.serializer is None
            assert q.deserializer is None

    class TestSerialization:
        """Tests the serialization and deserialization of messages."""

        def test_serialize_calls_serializer(self, conn: RedisLiteConnection, mocker):
            """You can serialize a message."""
            mocked_serializer = mocker.Mock()
            mocked_serializer.return_value = b"test"
            q = Queue(conn, "test_queue", serializer=mocked_serializer)
            assert q.serialize("test") == b"test"
            assert mocked_serializer.call_count == 1
            mocked_serializer.assert_called_with("test")

        def test_serialize_returns_message_without_serializer(self, conn: RedisLiteConnection):
            """Attempting to serialize a message without a serializer returns the message."""
            q = Queue(conn, "test_queue")
            assert q.serialize("test") == "test"

        def test_serialize_wraps_errors(self, conn: RedisLiteConnection, mocker):
            """Attempting to serialize a message with a serializer that raises an exception raises an exception."""
            mocked_serializer = mocker.Mock(__name__="test_serializer")
            mocked_serializer.side_effect = Exception("test")
            q = Queue(conn, "test_queue", serializer=mocked_serializer)
            with pytest.raises(UnserializableMessage):
                q.serialize("test")

        def test_deserialize_calls_deserializer(self, conn: RedisLiteConnection, mocker):
            """You can deserialize a message."""
            mocked_deserializer = mocker.Mock()
            mocked_deserializer.return_value = "test"
            q = Queue(conn, "test_queue", deserializer=mocked_deserializer)
            assert q.deserialize(b"test") == "test"
            assert mocked_deserializer.call_count == 1
            mocked_deserializer.assert_called_with(b"test")

        def test_deserialize_returns_message_without_deserializer(self, conn: RedisLiteConnection):
            """Attempting to deserialize a message with a deserializer that raises an exception raises an exception."""
            q = Queue(conn, "test_queue")
            assert q.deserialize(b"test") == b"test"

        def test_deserialize_wraps_errors(self, conn: RedisLiteConnection, mocker):
            """Attempting to deserialize a message with a deserializer that raises an exception raises an exception."""
            mocked_deserializer = mocker.Mock(__name__="test_deserializer")
            mocked_deserializer.side_effect = Exception("test")
            q = Queue(conn, "test_queue", deserializer=mocked_deserializer)
            with pytest.raises(UndeserializableMessage):
                q.deserialize(b"test")

    class TestGetMessage:
        def test_leaves_message_on_queue(self, conn: RedisLiteConnection):
            """Test get method leaves the message on the queue."""
            q = Queue(conn, "test_queue")
            msg_id = q.push("test")
            assert msg_id is not None
            assert q.metadata().msgs == 1
            msg = q.get()
            assert msg.message_id == msg_id
            assert msg.data == "test"
            assert q.metadata().msgs == 1

        def test_on_empty_queue_returns_none(self, conn: RedisLiteConnection):
            """Test get method on an empty queue returns None."""
            q = Queue(conn, "test_queue")
            assert q.get() is None

        def test_with_raise_on_empty_raises_error(self, conn: RedisLiteConnection):
            """Test get method with raise_on_empty=True raises an error."""
            q = Queue(conn, "test_queue")
            with pytest.raises(NoMessageInQueue):
                q.get(raise_on_empty=True)

    class TestDeleteMessage:
        def test_deletes_message_from_queue(self, conn: RedisLiteConnection):
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

        def test_is_idempotent(self, conn: RedisLiteConnection):
            """Test delete method is idempotent."""
            q = Queue(conn, "test_queue")
            msg_id = q.push("test")
            assert msg_id is not None
            assert q.metadata().msgs == 1
            q.delete(msg_id)
            assert q.metadata().msgs == 0
            q.delete(msg_id)
            assert q.metadata().msgs == 0

    def test_push(self, conn: RedisLiteConnection):
        """Test push method."""
        q = Queue(conn, "test_queue")
        msg_id = q.push("test")
        assert msg_id is not None
        assert q.metadata().msgs == 1

    def test_push_many(self, conn: RedisLiteConnection):
        """Test push_many method."""
        q = Queue(conn, "test_queue")
        msg_ids = q.push_many(["test", "test2"])
        assert len(msg_ids) == 2
        assert q.metadata().msgs == 2

    def test_pop_returns_message_and_deletes_it(self, conn: RedisLiteConnection):
        """Test pop method."""
        q = Queue(conn, "test_queue")
        msg_id = q.push("test")
        assert msg_id is not None
        assert q.metadata().msgs == 1
        msg = q.pop()
        assert msg.message_id == msg_id
        assert msg.data == "test"
        assert q.metadata().msgs == 0

    def test_pop_on_empty_queue_returns_none(self, conn: RedisLiteConnection):
        """Test pop method on an empty queue returns None."""
        q = Queue(conn, "test_queue")
        assert q.pop() is None
