"""
Definitions of data structures and constants.
"""
from dataclasses import dataclass

DEFAULT_VT = 60
"""The default visibility timeout for new queues."""

DEFAULT_DELAY = 0
"""The default delivery delay for new queues."""

DEFAULT_MAX_SIZE = 65565
"""The default maximum size of a message for new queues."""

DEFAULT_RETRIES = 5
"""The default number of message retries for new queues."""


@dataclass
class QueueOrder:
    """The order in which to return messages from a queue."""


@dataclass
class QueueAttributes:
    """
    The attributes, counter and stats of a queue.
    """

    viz_timeout: int
    """The length of time, in seconds, that a message received from a queue will
    be invisible to other receiving components when they ask to receive messages."""

    initial_delay: int
    "The time in seconds that the delivery of all new messages in the queue will be delayed"

    maxsize: int
    "The maximum size of a message in bytes"

    totalrecv: int
    "Total number of messages received from (taken off of) the queue"

    totalsent: int
    "Total number of messages sent to this queue"

    created: int
    "Timestamp (epoch in seconds) when the queue was created"

    modified: int
    "Timestamp (epoch in seconds) when the queue was last modified with :meth:`.Queue.set_attributes`"

    msgs: int
    "Current number of messages in the queue"

    hiddenmsgs: int
    """
    Current number of hidden / not visible messages.

    A message typically is hidden while "in flight". This number can be a good measurement for
    how many messages are currently being processed.
    """


@dataclass
class QueueDefinition:
    """
    A representation of RSMQ's internal :class:`.Queue` definition.
    """

    qname: str
    "The name of the queue"

    vt: int
    """The length of time, in seconds, that a message received from a queue will
    be invisible to other receiving components when they ask to receive messages."""

    delay: int
    "The time in seconds that the delivery of all new messages in the queue will be delayed"

    maxsize: int
    "The maximum size of a message in bytes"

    ts: int
    """The current Redis server timestamp in seconds since epoch.

    This value is updated each time :attr:`.Queue.definition` is accessed."""

    ts_usec: int
    """The current Redis server timestamp in microseconds since epoch.

    This value is updated each time :attr:`.Queue.definition` is accessed."""


@dataclass
class Message:
    """
    Receive the next message from the queue.
    """

    message_id: str
    "The internal message id."

    message: str
    "The message's contents."

    sent: int
    "Timestamp of when this message was sent/created."

    fr: int
    "Timestamp of when this message was first received."

    rc: int
    "Number of times this message was received."
