"""Connection managers for queues."""
import sqlite3
from pathlib import Path
from sqlite3 import Connection
from typing import Optional

from redislite import Redis

from psmq.exceptions import QueueDoesNotExist

from itertools import islice


def list_to_dict(l: list) -> dict:
    """Convert a list of alternating key-value pairs into a dict."""

    def batched(iterable, n) -> tuple:
        it = iter(iterable)
        while batch := tuple(islice(it, n)):
            yield batch

    r_str = []
    for item in l:
        r_str.append(item.decode("utf8") if isinstance(item, bytes) else item)
    return dict(batched(r_str, 2))


def setup_queue_db():
    """
    Idempotently setup the database for a queue.

    create 3 tables: queue, messages, and config

    Args:
        name: The name of the queue
        location: The path to the enclosing directory for the queue db file
        visibility_timeout: The visibility timeout
        delay: The delay
        max_size: The maximum size of a message
        retries: The number of retries
    """
    pass


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
    return sqlite3.connect(db_name)


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

        results = self._connection.fcall("get_queue_info", 1, queue_name)
        config = QueueConfiguration(int(results[0]), int(results[1]), int(results[2]))
        metadata = QueueMetadata(
            **{
                "created": int(results[3]),
                "modified": int(results[4]),
                "totalrecv": int(results[5]),
                "totalsent": int(results[6]),
                "msgs": int(results[7]),
                "hiddenmsgs": int(results[8]),
            }
        )
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

    def push_message(self, queue_name: str, message: bytes, delay: int = 0, ttl: int = 0) -> str:
        """Send a message to a queue."""
        return self._connection.fcall("push_message", 3, queue_name, message, delay).decode("utf8")

    def get_message(self, queue_name: str):
        """Get a message from a queue."""
        result = self._connection.fcall("get_message", 1, queue_name)
