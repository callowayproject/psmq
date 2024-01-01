"""Connection managers for queues."""
from contextlib import contextmanager
from datetime import datetime
from itertools import islice
from pathlib import Path
from typing import Any, Iterable, Optional, Tuple

import umsgpack
from redislite import Redis

from psmq.message import ReceivedMessage


def list_to_dict(kv_list: list) -> dict:
    """Convert a list of alternating key-value pairs into a dict."""

    def batched(iterable: Iterable) -> Iterable[Tuple[Any, Any]]:
        it = iter(iterable)
        while batch := tuple(islice(it, 2)):
            yield batch  # type: ignore[misc]

    r_str = []
    for item in kv_list:
        try:
            r_str.append(item.decode("utf8") if isinstance(item, bytes) else item)
        except UnicodeDecodeError:
            r_str.append(item)
    return dict(batched(r_str))


class RedisLiteConnection:
    """Queues backed by RedisLite."""

    def __init__(self, db_location: Path):
        self.psmq_library_file = Path(__file__).parent.joinpath("psmq_library.lua").read_text()
        self._db_location = db_location
        self.queue_set_key = "QUEUES"
        self._connection = Redis(self._db_location / "queue.rdb")
        self._connection.function_load(self.psmq_library_file)

    def list_queues(self) -> set:
        """List all queues."""
        return {name.decode("utf8") for name in self._connection.smembers(self.queue_set_key)}

    def create_queue(self, name: str, vt: int = 60, delay: int = 0, max_size: int = 65565) -> bool:
        """
        Create a queue.

        Args:
            name: the name of the queue
            vt: the visibility timeout
            delay: the initial delay
            max_size: the maximum size of a message

        Returns:
            True if the queue was created, False if it already exists
        """
        result = self._connection.fcall("create_queue", 4, name, vt, delay, max_size)
        return bool(result)

    def delete_queue(self, name: str) -> None:
        """Delete a queue."""
        self._connection.fcall("delete_queue", 1, name)

    def get_queue_info(self, queue_name: str) -> dict:
        """Get the config for a queue."""
        from .queue import QueueConfiguration, QueueMetadata

        results = list_to_dict(self._connection.fcall("get_queue_info", 1, queue_name))
        results = {key: int(value) for key, value in results.items()}
        config = QueueConfiguration(results.pop("vt"), results.pop("delay"), results.pop("maxsize"))
        metadata = QueueMetadata(**results)
        return {"config": config, "metadata": metadata}

    def set_queue_visibility_timeout(self, queue_name: str, vt: int) -> None:
        """Set the visibility timeout for a queue."""
        self._connection.fcall("set_queue_viz_timeout", 2, queue_name, vt)

    def set_queue_initial_delay(self, queue_name: str, delay: int) -> None:
        """Set the initial delay for a queue."""
        self._connection.fcall("set_queue_initial_delay", 2, queue_name, delay)

    def set_queue_max_size(self, queue_name: str, max_size: int) -> None:
        """Set the max size for a queue."""
        self._connection.fcall("set_queue_max_size", 2, queue_name, max_size)

    def push_message(
        self,
        queue_name: str,
        message: bytes,
        delay: Optional[int] = None,
        ttl: Optional[int] = None,
        metadata: Optional[dict] = None,
    ) -> str:
        """Send a message to a queue."""
        if metadata is None:
            metadata = umsgpack.packb({})
        elif isinstance(metadata, dict):
            metadata = umsgpack.packb(metadata)
        else:
            raise TypeError("metadata must be a dict")
        if delay is None:
            delay = -1
        return self._connection.fcall("push_message", 4, queue_name, message, delay, metadata).decode("utf8")

    def get_message(self, queue_name: str, visibility_timeout: Optional[int] = None) -> Optional[ReceivedMessage]:
        """Get a message from a queue."""
        if visibility_timeout is not None:
            msg_dict = list_to_dict(self._connection.fcall("get_message", 2, queue_name, visibility_timeout))
        else:
            msg_dict = list_to_dict(self._connection.fcall("get_message", 1, queue_name))
        if msg_dict:
            metadata = umsgpack.unpackb(msg_dict["metadata"])
            metadata["sent"] = datetime.fromtimestamp(int(metadata["sent"]))
            return ReceivedMessage(
                queue_name=queue_name,
                message_id=msg_dict["msg_id"],
                data=msg_dict["msg_body"],
                metadata=metadata,
                sent=metadata["sent"],
                first_retrieved=datetime.fromtimestamp(int(msg_dict["fr"]) / 1000),
                retrieval_count=msg_dict["rc"],
            )
        return None

    def delete_message(self, queue_name: str, msg_id: str) -> None:
        """Delete a message from a queue."""
        self._connection.fcall("delete_message", 2, queue_name, msg_id)

    def pop_message(self, queue_name: str) -> dict:
        """Get and delete a message from a queue."""
        return list_to_dict(self._connection.fcall("pop_message", 1, queue_name))

    @contextmanager
    def pipeline(self) -> Iterable[Redis]:
        """Create a transaction pipeline."""
        tx = self._connection.pipeline(transaction=True)
        yield
        tx.execute()
