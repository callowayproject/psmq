"""Serialize and deserialize messages."""

import json
from typing import Any, Callable

SerializerFunc = Callable[[Any], bytes]
DeserializerFunc = Callable[[bytes], Any]


def default_serializer(message: Any) -> bytes:
    """Serialize a message using JSON."""
    return json.dumps(message).encode()


def default_deserializer(message: bytes) -> Any:
    """Deserialize a message using JSON."""
    if isinstance(message, bytes):
        return json.loads(message.decode("utf-8"))
    else:
        return json.loads(message)
