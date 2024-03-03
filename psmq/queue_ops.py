"""Queue Operations for Redis connections."""

from datetime import datetime
from typing import Optional, Union

import umsgpack
from redis.client import Pipeline, Redis

from psmq.message import ReceivedMessage
from psmq.utils import list_to_dict
from psmq.validation import validate_queue_name

QUEUE_SET_KEY = "QUEUES"


def create_queue(conn: Redis, name: str, vt: int = 60, delay: int = 0, max_size: int = 65565) -> bool:
    """
    Create a queue.

    Args:
        conn: the Redis connection
        name: the name of the queue
        vt: the visibility timeout
        delay: the initial delay
        max_size: the maximum size of a message

    Returns:
        True if the queue was created, False if it already exists
    """
    validate_queue_name(name)
    result = conn.fcall("create_queue", 4, name, vt, delay, max_size)  # type: ignore[attr-defined]
    return bool(result)


def delete_queue(conn: Redis, name: str) -> None:
    """Delete a queue and all its messages."""
    conn.fcall("delete_queue", 1, name)  # type: ignore[attr-defined]


def list_queues(conn: Redis) -> set:
    """List all queues."""
    return {name.decode("utf8") for name in conn.smembers(QUEUE_SET_KEY)}


def get_queue_info(conn: Redis, queue_name: str) -> dict:
    """Get the config for a queue."""
    from psmq.queue import QueueConfiguration, QueueMetadata

    results = list_to_dict(conn.fcall("get_queue_info", 1, queue_name))  # type: ignore[attr-defined]
    results = {key: int(value) for key, value in results.items()}
    config = QueueConfiguration(results.pop("vt"), results.pop("delay"), results.pop("maxsize"))
    metadata = QueueMetadata(**results)
    return {"config": config, "metadata": metadata}


def set_queue_visibility_timeout(conn: Redis, queue_name: str, vt: int) -> None:
    """Set the visibility timeout for a queue."""
    conn.fcall("set_queue_viz_timeout", 2, queue_name, vt)  # type: ignore[attr-defined]


def set_queue_initial_delay(conn: Redis, queue_name: str, delay: int) -> None:
    """Set the initial delay for a queue."""
    conn.fcall("set_queue_initial_delay", 2, queue_name, delay)  # type: ignore[attr-defined]


def set_queue_max_size(conn: Redis, queue_name: str, max_size: int) -> None:
    """Set the max size for a queue."""
    conn.fcall("set_queue_max_size", 2, queue_name, max_size)  # type: ignore[attr-defined]


def push_message(
    conn: Union[Redis, Pipeline],
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
    ret_val = conn.fcall(  # type: ignore[attr-defined,union-attr]
        "push_message", 4, queue_name, message, delay, metadata
    )
    if isinstance(ret_val, bytes):
        return ret_val.decode("utf8")
    else:
        return ret_val


def get_message(conn: Redis, queue_name: str, visibility_timeout: Optional[int] = None) -> Optional[ReceivedMessage]:
    """Get a message from a queue."""
    if visibility_timeout is not None:
        msg_dict = list_to_dict(
            conn.fcall("get_message", 2, queue_name, visibility_timeout)  # type: ignore[attr-defined]
        )
    else:
        msg_dict = list_to_dict(conn.fcall("get_message", 1, queue_name))  # type: ignore[attr-defined]
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


def delete_message(conn: Redis, queue_name: str, msg_id: str) -> None:
    """Delete a message from a queue."""
    conn.fcall("delete_message", 2, queue_name, msg_id)  # type: ignore[attr-defined]


def pop_message(conn: Redis, queue_name: str) -> dict:
    """Get and delete a message from a queue."""
    return list_to_dict(conn.fcall("pop_message", 1, queue_name))  # type: ignore[attr-defined]
