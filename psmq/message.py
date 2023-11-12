"""Messages for PSMQ."""
from dataclasses import dataclass
from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .queue import Queue


@dataclass
class Message:
    """A message received from a Queue."""

    queue: "Queue"
    "The queue this message was received from."

    message_id: str
    "The internal message id."

    data: Any
    "The message's contents."

    sent: int
    "Timestamp of when this message was sent/created."

    first_retrieved: int
    "Timestamp of when this message was first received."

    retrieval_count: int
    "The number of times this message has been retrieved."

    ttl: Optional[int] = None
    "The message's time-to-live in microseconds."

    def delete(self) -> None:
        """
        Delete this message from the queue.
        """
        self.queue.delete(self.message_id)

    @property
    def expires(self) -> Optional[int]:
        """
        Timestamp of when this message will expire. This is calculated from the message's `ttl`.
        """
        return None if self.ttl is None else self.sent + self.ttl
