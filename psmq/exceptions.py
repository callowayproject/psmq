"""Exceptions raised."""

from typing import Any, Union


class PSMQError(Exception):
    """
    Base class for all PSMQ exceptions.
    """

    pass


class QueueAlreadyExists(PSMQError):
    """
    A queue already exists with that name.

    Raised when attempting to create a :class:`~psmq.queue.Queue` with the same
    name as an existing :class:`~psmq.queue.Queue` on the Redis server
    """

    def __init__(self, qname: str):
        super().__init__(f"Queue '{qname}' already exists.")


class QueueDoesNotExist(PSMQError):
    """
    Raised when accessing a :class:`~psmq.queue.Queue` that does not exist.
    """

    def __init__(self, qname: str):
        super().__init__(f"Queue '{qname}' does not exist.")


class NoMessageInQueue(PSMQError):
    """
    Raised when a call does not revceive a message.

    """

    def __init__(self, qname: str):
        super().__init__(f"There are no messages in queue '{qname}'.")


class InvalidQueueName(PSMQError):
    """
    The base exception for errors relating to :class:`~psmq.queue.Queue` names.
    """

    pass


class InvalidCharacter(InvalidQueueName):
    """
    Raised when creating a :class:`~psmq.queue.Queue` with invalid characters in its name.

    The valid characters are defined in :attr:`psmq.validation.QNAME_INVALID_CHARS_RE`
    """

    def __init__(self, character: str):
        super().__init__(f"The '{character}' character is not allowed in queue names.")


class QueueNameTooLong(InvalidQueueName):
    """
    Raised when creating a :class:`~psmq.queue.Queue` with too many characters in its name.

    The maximum length is defined in :attr:`psmq.validation.QNAME_MAX_LENGTH`
    """

    def __init__(self, max_length: int):
        super().__init__(f"The queue name must be shorter than {max_length} characters.")


class ValueTooLow(ValueError):
    """
    Raised when a numerical value is lower than a specified minumum.
    """

    def __init__(self, min_val: Union[int, float]):
        super().__init__(f"The value must not be lower than {min_val}.")


class ValueTooHigh(ValueError):
    """
    Raised when a numerical value is greater than a specified maximum.
    """

    def __init__(self, max_val: Union[int, float]):
        super().__init__(f"The value must not be higher than {max_val}.")


class UnserializableMessage(PSMQError):
    """
    Raised when a message cannot be serialized.
    """

    def __init__(self, message: Any, serializer: str):
        super().__init__(f"Cannot serialize message `{message!r}` with {serializer}")


class UndeserializableMessage(PSMQError):
    """
    Raised when a message cannot be deserialized.
    """

    def __init__(self, message: bytes, deserializer: str):
        super().__init__(f"Cannot deserialize message `{message!r}` with {deserializer}")
