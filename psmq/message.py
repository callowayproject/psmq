"""Messages for PSMQ."""
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional


@dataclass
class ReceivedMessage:
    """A message received from a Queue."""

    queue_name: str
    "The queue this message was received from."

    message_id: str
    "The internal message id."

    data: Any
    "The message's contents."

    sent: datetime
    "Timestamp of when this message was sent/created."

    first_retrieved: datetime
    "Timestamp of when this message was first received."

    retrieval_count: int
    "The number of times this message has been retrieved."

    ttl: Optional[int] = None
    "The message's time-to-live in seconds."

    # def delete(self) -> None:
    #     """
    #     Delete this message from the queue.
    #     """
    #     self.queue.delete(self.message_id)

    @property
    def expires(self) -> Optional[datetime]:
        """
        Timestamp of when this message will expire. This is calculated from the message's `ttl`.
        """
        return None if self.ttl is None else self.sent + timedelta(seconds=self.ttl)
