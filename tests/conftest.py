from pathlib import Path

import pytest
from redis.client import Redis

from psmq.connection import get_redis_from_path


@pytest.fixture
def conn(tmp_path: Path) -> Redis:
    """Get a RedisLite connection."""
    return get_redis_from_path(tmp_path)
