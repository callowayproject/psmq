"""Queue mangement."""
import logging
from functools import cache
from pathlib import Path
from typing import Optional

from .connection import RedisLiteConnection
from .queue import Queue, QueueConfiguration
from .serialize import SerializerFunc, DeserializerFunc, default_serializer, default_deserializer

logger = logging.getLogger(__name__)


class QueueManager:
    """A manager for queues."""

    def __init__(self, db_dir: Path):
        """
        Args:
            db_dir: The path to the directory containing the queue db files
        """
        self.db_dir = db_dir
        self.connection = RedisLiteConnection(db_location=db_dir)

    @cache
    def get_queue(
        self,
        name: str,
        default_config: Optional[QueueConfiguration] = None,
        serializer: Optional[SerializerFunc] = None,
        deserializer: Optional[DeserializerFunc] = None,
    ) -> Queue:
        """
        Get or create a queue by name.

        Args:
            name: The name of the queue
            default_config: The default configuration to use if the queue does not exist
            serializer: Optional method to serialize messages
            deserializer: Optional method to deserialize messages

        Raises:
            InvalidQueueName: If ``name`` is invalid

        Returns:
            The queue
        """
        if default_config:
            self.connection.create_queue(
                name, default_config.visibility_timeout, default_config.initial_delay, default_config.max_size
            )
        else:
            self.connection.create_queue(name)

        if not serializer:
            serializer = default_serializer
        if not deserializer:
            deserializer = default_deserializer

        return Queue(
            connection=self.connection,
            name=name,
            serializer=serializer,
            deserializer=deserializer,
        )

    def delete_queue(self, name: str) -> None:
        """
        Delete a queue and all its messages.

        Args:
            name: The name of the queue
        """
        self.connection.delete_queue(name)

    def queues(self) -> set:
        """
        List all queues.

        Returns:
            The set of queue names
        """
        return self.connection.list_queues()
