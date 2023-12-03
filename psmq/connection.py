"""Connection managers for queues."""
import sqlite3
from datetime import datetime
from itertools import islice
from pathlib import Path
from sqlite3 import Connection
from typing import Any, Iterable, Optional, Tuple

import umsgpack
from redislite import Redis

from psmq.exceptions import QueueDoesNotExist
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


def get_db_connection(name: str, location: Optional[Path] = None, raise_if_missing: bool = False) -> Connection:
    """
    Create a connection to a persistent or ephemeral database.

    Args:
        name: The name of the queue
        location: The path to the enclosing directory for the queue db file. ``None`` if ephemeral db.
        raise_if_missing: If ``True``, do not attempt to create the db and raise an error

    Raises:
        QueueDoesNotExist: If ``raise_if_missing`` is ``True`` and the DB is missing.

    Returns:
        The sqlite connection
    """
    if location is None and raise_if_missing:
        # Ephemeral DBs are always missing
        raise QueueDoesNotExist(name)
    elif location and raise_if_missing:
        queue_file = location / f"{name}.db"
        if not queue_file.exists():
            raise QueueDoesNotExist(name)
    db_name = location / f"{name}.db" if location else ":memory:"
    return sqlite3.connect(str(db_name))


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
        return self._connection.smembers(self.queue_set_key)

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
        self, queue_name: str, message: bytes, delay: Optional[int] = None, ttl: Optional[int] = None
    ) -> str:
        """Send a message to a queue."""
        if delay is not None:
            return self._connection.fcall("push_message", 3, queue_name, message, delay).decode("utf8")
        return self._connection.fcall("push_message", 2, queue_name, message).decode("utf8")

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
