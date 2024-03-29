"""Validation functions."""

import re
from typing import Any, Optional

from .exceptions import (
    InvalidCharacter,
    InvalidQueueName,
    QueueNameTooLong,
    ValueTooHigh,
    ValueTooLow,
)

#: :obj:`re.Pattern`: A compiled regular expression that detects all invalid characters
QNAME_INVALID_CHARS_RE = re.compile(r"[^A-Za-z0-9._-]")

#: int: The maximum number of characters allowed in a :class:`~rsmq.queue.Queue` name
QNAME_MAX_LENGTH = 160


def validate_queue_name(qname: str, raise_on_error: bool = False) -> bool:
    """
    Verify that the passed in queue name is valid.

    Args:
        qname: The name of the queue to validate
        raise_on_error: If ``False``, return a ``bool`` instead of raising exceptions on errors

    Raises:
        InvalidQueueName: If ``qname`` is missing and ``raise_on_error`` is ``True``
        QueueNameTooLong: If ``qname`` is longer than :data:`~.QNAME_MAX_LENGTH` and ``raise_on_error`` is ``True``
        InvalidCharacter: If ``qname`` contains characters found in :data:`~.QNAME_INVALID_CHARS_RE`
            and ``raise_on_error`` is ``True``

    Returns:
        ``True`` if valid, ``False`` otherwise if ``raise_on_error`` is ``False``
    """
    if not qname:
        if raise_on_error:
            raise InvalidQueueName("Queue name cannot be empty.")
        else:
            return False

    if len(qname) > QNAME_MAX_LENGTH:
        if raise_on_error:
            raise QueueNameTooLong(QNAME_MAX_LENGTH)
        else:
            return False

    invalid_chars = QNAME_INVALID_CHARS_RE.search(qname)
    if invalid_chars:
        if raise_on_error:
            raise InvalidCharacter(qname[invalid_chars.span()[0]])
        else:
            return False
    return True


def validate_int(
    value: Any,
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
    raise_on_error: bool = False,
) -> bool:
    """
    Validate value is integer and between min and max values (if specified).

    Raises:
        TypeError: If ``value`` is not an ``int``
        ValueTooLow: If ``value`` is lower than a specified ``min_value``
        ValueTooHigh: If ``value`` is greater than a specified ``max_value``

    Args:
        value: The value to validate
        min_value: If specified, the integer must be greater than or equal to this value
        max_value: If specified, the integer must be less than or equal to this value
        raise_on_error: If False, return a ``bool`` instead of raising exceptions on errors

    Returns:
        ``True`` if valid, ``False`` otherwise if ``raise_on_error`` is ``False``
    """
    if value is None or not isinstance(value, int):
        if raise_on_error:
            raise TypeError("An integer value is required.")
        else:
            return False

    if min_value is not None and value < min_value:
        if raise_on_error:
            raise ValueTooLow(min_value)
        else:
            return False

    if max_value is not None and value > max_value:
        if raise_on_error:
            raise ValueTooHigh(max_value)
        else:
            return False

    return True


def validate_float(
    value: Any,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
    quiet: bool = False,
) -> bool:
    """
    Validate value is integer and between min and max values (if specified).

    Raises:
        TypeError: If ``value`` is not an ``int``
        ValueTooLow: If ``value`` is lower than a specified ``min_value``
        ValueTooHigh: If ``value`` is greater than a specified ``max_value``

    Args:
        value: The value to validate
        min_value: If specified, the float must be greater than or equal to this value
        max_value: If specified, the float must be less than or equal to this value
        quiet: If True, return a ``bool`` instead of raising exceptions on errors

    Returns:
        ``True`` if valid, ``False`` otherwise if ``quiet`` is ``True``
    """
    if value is None or not isinstance(value, (int, float)):
        if quiet:
            return False
        raise TypeError("An integer or float value is required.")

    if min_value is not None and value < min_value:
        if quiet:
            return False
        raise ValueTooLow(min_value)

    if max_value is not None and value > max_value:
        if quiet:
            return False
        raise ValueTooHigh(max_value)

    return True
