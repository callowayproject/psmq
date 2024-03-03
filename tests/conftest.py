from pathlib import Path

import pytest

from psmq.connection import get_redis_from_path
from redis.client import Redis


@pytest.fixture
def conn(tmp_path: Path) -> Redis:
    """Get a RedisLite connection."""
    return get_redis_from_path(tmp_path)
