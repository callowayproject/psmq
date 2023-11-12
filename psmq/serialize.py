"""Serialize and deserialize messages."""
from typing import Callable, Any
import json

SerializerFunc = Callable[[Any], bytes]
DeserializerFunc = Callable[[bytes], Any]


def default_serializer(message: Any) -> bytes:
    """Serialize a message using JSON."""
    return json.dumps(message).encode()


def default_deserializer(message: bytes) -> Any:
    """Deserialize a message using JSON."""
    return json.loads(message.decode())
