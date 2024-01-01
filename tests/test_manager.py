"""Tests the QueueManager class."""
from pathlib import Path

from psmq.manager import QueueManager
from psmq.queue import Queue, QueueConfiguration


def dummy_serializer(msg: str) -> bytes:
    """A dummy serializer."""
    return msg.encode()


def dummy_deserializer(msg: bytes) -> str:
    """A dummy deserializer."""
    return msg.decode()


class TestQueueManager:
    """Tests the QueueManager class."""

    def test_instantiation(self, tmp_path: Path):
        """You can instantiate a QueueManager object with only a path."""
        qm = QueueManager(tmp_path)
        assert qm.connection is not None

    class TestGetQueue:
        """Tests the get_queue method."""

        def test_returns_queue(self, tmp_path: Path):
            """You can get a queue from the QueueManager."""
            qm = QueueManager(tmp_path)
            q = qm.get_queue("test_queue")
            assert isinstance(q, Queue)
            assert q.name == "test_queue"

        def test_returns_same_queue(self, tmp_path: Path):
            """Multiple attempts to get the same queue returns the same queue."""
            qm = QueueManager(tmp_path)
            q = qm.get_queue("test_queue")
            q2 = qm.get_queue("test_queue")
            assert q is q2

        def test_supports_custom_configuration(self, tmp_path: Path):
            """You can specify a custom configuration for a queue."""
            qm = QueueManager(tmp_path)
            qc = QueueConfiguration(visibility_timeout=500)
            q = qm.get_queue(name="test_queue", default_config=qc)
            assert q._configuration.visibility_timeout == 500

        def test_supports_custom_serializers(self, tmp_path: Path):
            """You can specify a custom serializer and deserializer for a queue."""
            qm = QueueManager(tmp_path)
            q = qm.get_queue(name="test_queue", serializer=dummy_serializer, deserializer=dummy_deserializer)
            assert q.serializer == dummy_serializer
            assert q.deserializer == dummy_deserializer

    class TestDeleteQueue:
        """Tests the delete_queue method."""

        def test_deletes_queue(self, tmp_path: Path):
            """You can delete a queue."""
            qm = QueueManager(tmp_path)
            qm.get_queue("test_queue")
            assert "test_queue" in qm.queues()
            qm.delete_queue("test_queue")
            assert "test_queue" not in qm.queues()

    class TestListQueues:
        """Tests the list_queues method."""

        def test_returns_a_set_of_queue_names(self, tmp_path: Path):
            """You can list the queues."""
            qm = QueueManager(tmp_path)
            assert qm.queues() == set()

            qm.get_queue("test_queue")
            assert qm.queues() == {"test_queue"}

            qm.get_queue("test_queue2")
            assert qm.queues() == {"test_queue", "test_queue2"}

            qm.delete_queue("test_queue")
            assert qm.queues() == {"test_queue2"}
