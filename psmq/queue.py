"""Queues and message handling."""

from dataclasses import dataclass
from typing import Any, List, Optional

from redis import Redis

from psmq import queue_ops
from psmq.exceptions import NoMessageInQueue, UndeserializableMessage, UnserializableMessage
from psmq.message import ReceivedMessage
from psmq.serialize import DeserializerFunc, SerializerFunc, default_deserializer, default_serializer
from psmq.validation import validate_queue_name


@dataclass(frozen=True)
class QueueConfiguration:
    """Configuration for a queue."""

    visibility_timeout: int = 60
    """The length of time, in seconds, that a message received from a queue will
    be invisible to other receiving components when they ask to receive messages."""

    initial_delay: int = 0
    "The time in seconds that the delivery of all new messages in the queue will be delayed"

    max_size: int = 65565
    "The maximum size of a message in bytes"

    retries: int = 5
    "The number of times to retry a message before giving up."

    ttl: Optional[int] = None
    "The optional time to live for a message in milliseconds."


@dataclass(frozen=True)
class QueueMetadata:
    """Metadata for a queue."""

    totalrecv: int
    "Total number of messages received from (taken off of) the queue"

    totalsent: int
    "Total number of messages sent to this queue"

    created: int
    "Timestamp (epoch in seconds) when the queue was created"

    modified: int
    "Timestamp (epoch in seconds) when the queue was last modified"

    msgs: int
    "Current number of messages in the queue"

    hiddenmsgs: int
    """
    Current number of hidden / not visible messages.

    A message typically is hidden while "in flight". This number can be a good measurement for
    how many messages are currently being processed.
    """


class Queue:
    """
    Representation of a specific Queue in Redis.

    Args:
        connection: The root connection object
        name: The name of the queue
        default_config: The default configuration to use if the queue does not exist
        serializer: Optional method to serialize messages
        deserializer: Optional method to deserialize messages
    """

    def __init__(
        self,
        connection: Redis,
        name: str,
        default_config: Optional[QueueConfiguration] = None,
        serializer: Optional["SerializerFunc"] = None,
        deserializer: Optional["DeserializerFunc"] = None,
    ):
        validate_queue_name(name, raise_on_error=True)
        self.connection = connection
        self.name = name
        if default_config:
            queue_ops.create_queue(
                connection,
                name,
                default_config.visibility_timeout,
                default_config.initial_delay,
                default_config.max_size,
            )
        else:
            queue_ops.create_queue(connection, name)
        q_info = queue_ops.get_queue_info(self.connection, name)
        self._configuration = q_info["config"]
        self._metadata = q_info["metadata"]
        self.serializer = serializer or default_serializer
        self.deserializer = deserializer or default_deserializer

    def metadata(self) -> QueueMetadata:
        """
        Get the metadata for the queue.

        Returns:
            The queue metadata
        """
        q_info = queue_ops.get_queue_info(self.connection, self.name)
        return q_info["metadata"]

    def serialize(self, message: Any) -> bytes:
        """
        Serialize a message.

        Args:
            message: The message to serialize

        Raises:
            UnserializableMessage: If the message cannot be deserialized

        Returns:
            The serialized message
        """
        try:
            return self.serializer(message)
        except Exception as e:  # NOQA: BLE001
            # TODO: This might be a place to provide a hook or configuration for error handling
            raise UnserializableMessage(message, self.serializer.__name__) from e

    def deserialize(self, message: bytes) -> Any:
        """
        Deserialize a message.

        Args:
            message: The message to deserialize

        Raises:
            UndeserializableMessage: If the message cannot be deserialized

        Returns:
            The deserialized message
        """
        try:
            return self.deserializer(message)
        except Exception as e:  # NOQA: BLE001
            # TODO: This might be a place to provide a hook or configuration for error handling
            raise UndeserializableMessage(message, self.deserializer.__name__) from e

    def push(self, message: Any, delay: Optional[int] = None, ttl: Optional[int] = None) -> str:
        """
        Send a message to the queue.

        Args:
            message: The message to send
            delay: The time in seconds that
                the delivery of the message will be delayed. Allowed values: 0-9999999
                (around 115 days)
            ttl: The time to live for the message in milliseconds. Allowed values: 0-9999999

        Returns:
            The message id
        """
        serialized = self.serialize(message)
        return queue_ops.push_message(self.connection, self.name, serialized, delay=delay, ttl=ttl)

    def push_many(self, messages: List[Any], delay: Optional[int] = None, ttl: Optional[int] = None) -> list:
        """
        Send multiple messages, all pipelined together.

        Args:
            messages: The messages to send
            delay: The time in seconds that
                the delivery of the message will be delayed. Allowed values: 0-9999999
                (around 115 days)
            ttl: The time to live for the message in milliseconds. Allowed values: 0-9999999

        Returns:
            All message ids
        """
        tx = self.connection.pipeline(transaction=True)
        for message in messages:
            serialized = self.serialize(message)
            queue_ops.push_message(tx, self.name, serialized, delay=delay, ttl=ttl)
        message_ids = tx.execute()
        return [msg_id.decode("utf-8") for msg_id in message_ids]

    def delete(self, msg_id: str) -> None:
        """
        Delete a message if it exists.

        Args:
            msg_id: The ID of the message to delete
        """
        queue_ops.delete_message(self.connection, self.name, msg_id)

    def get(self, visibility_timeout: Optional[int] = None, raise_on_empty: bool = False) -> Optional[ReceivedMessage]:
        """
        Receive a message.

        Args:
            visibility_timeout: optional (Default: queue settings) The length of time, in seconds,
                that the received message will be invisible to others. Allowed values:
                0-9999999 (around 115 days)
            raise_on_empty: optional (Default: False) Raise an exception if there is no message.

        Raises:
            NoMessageInQueue: If the queue was empty and ``raise_on_empty`` is ``True``

        Returns:
            The message if available, or ``None``
        """
        msg = queue_ops.get_message(self.connection, self.name, visibility_timeout=visibility_timeout)
        if msg is None and raise_on_empty:
            raise NoMessageInQueue(self.name)
        elif msg is None:
            return None
        else:
            msg.data = self.deserialize(msg.data) if msg.data else msg.data
            return msg

    def pop(self, raise_on_empty: bool = False) -> Optional[ReceivedMessage]:
        """
        Receive a message and delete it immediately.

        Args:
            raise_on_empty: optional (Default: False) Raise an exception if there is no message.

        Returns:
            The message if available, or ``None``
        """
        msg = self.get(raise_on_empty=raise_on_empty)
        if msg:
            self.delete(msg.message_id)
        return msg


# def handle_message_result(result: tuple) -> Message:
#     """
#     Handle a message received from the queue and format it properly.
#
#     Args:
#         result: The raw message data received from Redis
#
#     Returns:
#         A populated Message object
#     """
#     message_id, message, rc, fr = result
#     message = decode_message(message)
#     sent = base36decode(message_id[:10])
#     return Message(message_id, message, sent, fr, rc)


# def make_message_id(usec: int) -> str:
#     """
#     Need to create a unique id based on the redis timestamp and a random number.
#
#     The first part is the Redis time base-36 encoded which lets redis order the messages correctly
#     even when they are in the same millisecond.
#
#     Args:
#         usec: The Redis timestamp in microseconds as an integer
#
#     Returns:
#         A time-sortable, unique message id
#     """
#     import random
#
#     charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
#
#     msg_id = [random.choice(charset) for _ in range(22)]  # nosec
#     msg_id.insert(0, base36encode(usec))
#     return "".join(msg_id)
