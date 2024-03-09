"""An interface for Redis Streams."""

import secrets
from dataclasses import dataclass
from typing import Any, List, Optional, Union

from redis import ResponseError
from redis.client import Redis

from psmq.utils import list_to_dict


def short_id() -> str:  # pragma: no-coverage
    """Generate a short ID."""
    alphabet = "23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"  # pragma: allowlist secret
    return "".join(secrets.choice(alphabet) for _ in range(8))


@dataclass
class ConsumerGroupInfo:
    """Information about a consumer group."""

    name: str
    """The consumer group's name."""

    consumers: str
    """The number of consumers in the group"""

    pending: int
    """The number of messages that were delivered but are yet to be acknowledged"""

    last_delivered_id: str
    """The ID of the last entry delivered to the group's consumers"""

    entries_read: int
    """The logical "read counter" of the last entry delivered to the group's consumers"""

    lag: int
    """The number of entries in the stream that are still waiting to be delivered to the group's consumers"""


class StreamMessage:
    """A Redis Stream message."""

    def __init__(self, message_id: bytes, fields: Union[dict, list]):
        self.message_id = message_id.decode("utf8")
        if isinstance(fields, dict):
            self.fields = {k.decode("utf8"): v.decode("utf8") for k, v in fields.items()}
        else:
            self.fields = list_to_dict([x.decode("utf8") for x in fields])

    def __getattr__(self, attr: Any) -> Any:
        """Treat the attribute as a key in the fields dict."""
        if attr in self.fields:
            return self.fields[attr]
        raise AttributeError(f"Attribute {attr} not found in message fields")

    def __repr__(self) -> str:
        """Return a string representation of the message."""
        return f"<StreamMessage {self.message_id} {self.fields}>"


class Stream:
    """A Redis Stream."""

    def __init__(
        self,
        connection: Redis,
        name: str,
    ):
        self.conn = connection
        self.stream_name = name

    def publish(self, fields: dict) -> str:
        """Publish a message to the stream."""
        return self.conn.xadd(self.stream_name, fields)

    def create_consumer_group(self, group_name: str, from_start: bool = True) -> None:
        """
        Create a new consumer group in Redis.

        Args:
            group_name: The name of the consumer group to create
            from_start: If the consumer group does not exist and `True`,
                the consumer group will start at the beginning of the stream.
        """
        try:
            message_id = "0-0" if from_start else "$"
            self.conn.xgroup_create(name=self.stream_name, groupname=group_name, id=message_id, mkstream=True)
        except ResponseError:
            pass  # Group already exists

    def list_consumer_groups(self) -> List[ConsumerGroupInfo]:
        """List all consumer groups for the stream."""
        consumers = self.conn.xinfo_groups(self.stream_name)
        return [
            ConsumerGroupInfo(
                name=group["name"],
                consumers=group["consumers"],
                pending=group["pending"],
                last_delivered_id=group["last-delivered-id"],
                entries_read=group["entries-read"],
                lag=group["lag"],
            )
            for group in consumers
        ]

    def delete_consumer_group(self, group_name: str) -> None:
        """
        Delete a consumer group.

        The consumer group will be destroyed even if there are active consumers, and pending messages,
        so make sure to call this command only when really needed.

        Args:
            group_name: The name of the consumer group to delete
        """
        self.conn.xgroup_destroy(self.stream_name, group_name)

    def set_group_offset_id(
        self,
        group_name: str,
        message_id: Optional[str] = None,
    ) -> None:
        """
        Set the last delivered message ID for a consumer group.

        Args:
            group_name: The name of the consumer group
            message_id: The ID of the last delivered message
        """
        self.conn.xgroup_setid(self.stream_name, group_name, message_id)

    def reset_group_offset(self, group_name: str) -> None:
        """Set the last-delivered-id of the group to the beginning."""
        self.conn.xgroup_delconsumer(self.stream_name, group_name, "0-0")

    def roll_back_group_offset(self, group_name: str, num_messages: int) -> None:
        """Set the last-delivered-id of the group to `num_messages` from the last added message."""
        self.conn.xgroup_setid(self.stream_name, group_name, "$", num_messages)

    def create_consumer(self, group_name: str, consumer_name: str) -> None:
        """Create a new consumer in a consumer group."""
        self.create_consumer_group(group_name)
        self.conn.xgroup_createconsumer(self.stream_name, group_name, consumer_name)


class Consumer:
    """A Redis Consumer."""

    def __init__(self, connection: Redis, stream_name: str, group_name: str, consumer_name: Optional[str] = None):
        self.conn = connection
        self.stream_name = stream_name
        self.stream = Stream(connection, stream_name)
        self.group_name = group_name
        self.consumer_name = consumer_name or short_id()
        self.max_pending_time = 3000  # 3 seconds
        self._setup()

    def _setup(self) -> None:
        """Create the consumer group and consumer if they don't exist."""
        self.stream.create_consumer_group(self.group_name)
        self.stream.create_consumer(self.group_name, self.consumer_name)

    def get(self, count: int = 1, timeout: Optional[int] = None) -> list:
        """
        Consume messages from the stream.

        Messages are not acknowledged and require an explicit call to `ack` for each message id.

        Args:
            count: The number of messages to consume
            timeout: The number of milliseconds to wait for messages. None means do not wait.

        Returns:
            A list of messages received within the timeout
        """
        claimed_msgs = self.autoclaim(count)
        if len(claimed_msgs) == count:
            return claimed_msgs
        else:
            remaining = count - len(claimed_msgs)
            messages = self.conn.xreadgroup(
                self.group_name, self.consumer_name, {self.stream_name: ">"}, count=remaining, block=timeout
            )
            messages = messages or [("", [])]
            stream_msgs = messages[0][1]
            msgs = [StreamMessage(message_id, fields) for message_id, fields in stream_msgs]
            return claimed_msgs + msgs

    def autoclaim(self, count: int = 1, pending_ms: Optional[int] = None) -> list:
        """
        Claim idle messages from the stream.

        Args:
            count: The number of messages to claim
            pending_ms: The number of milliseconds messages should be pending before being claimed.
                Defaults to 3 seconds.

        Returns:
            A list of messages claimed
        """
        autoclaimed = self.conn.xautoclaim(
            self.stream.stream_name,
            self.group_name,
            self.consumer_name,
            pending_ms or self.max_pending_time,
            start_id="0-0",
            count=count,
        )
        if len(autoclaimed) < 2:
            return []
        claimed_msgs = autoclaimed[1]
        return [StreamMessage(message_id, fields) for message_id, fields in claimed_msgs]

    def ack(self, message_ids: Union[str, List[str]]) -> None:
        """Acknowledge a message."""
        if isinstance(message_ids, str):
            message_ids = [message_ids]
        self.conn.xack(self.stream_name, self.group_name, *message_ids)

    def pop(self, count: int = 1, timeout: Optional[int] = None) -> list:
        """
        Pop messages from the stream.

        Messages are acknowledged automatically when read.

        Args:
            count: The number of messages to pop
            timeout: The number of milliseconds to wait for messages. None means do not wait.

        Returns:
            A list of messages received within the timeout
        """
        messages = self.conn.xreadgroup(
            self.group_name,
            self.consumer_name,
            streams={self.stream_name: ">"},
            count=count,
            block=timeout,
            noack=True,
        )

        messages = messages or [("", [])]
        stream_msgs = messages[0][1]
        return [StreamMessage(message_id, fields) for message_id, fields in stream_msgs]
