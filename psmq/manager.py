"""Queue mangement."""
import logging
from pathlib import Path
from typing import Optional

from .connection import RedisLiteConnection
from .exceptions import QueueDoesNotExist
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
        self.redis_queues = RedisLiteConnection(db_location=db_dir)

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
        self.redis_queues.create_queue(
            name, default_config.visibility_timeout, default_config.initial_delay, default_config.max_size
        )

        if not serializer:
            serializer = default_serializer
        if not deserializer:
            deserializer = default_deserializer

        return Queue(
            connection=self.redis_queues,
            name=name,
            serializer=serializer,
            deserializer=deserializer,
        )
