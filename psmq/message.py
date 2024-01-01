"""Messages for PSMQ."""
from dataclasses import dataclass, field
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

    metadata: dict = field(default_factory=dict)

    @property
    def expires(self) -> Optional[datetime]:
        """
        Timestamp of when this message will expire. This is calculated from the message's `ttl`.
        """
        return None if self.ttl is None else self.sent + timedelta(seconds=self.ttl)
