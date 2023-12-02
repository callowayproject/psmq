from pathlib import Path

import pytest

from psmq.connection import RedisLiteConnection


@pytest.fixture
def conn(tmp_path: Path):
    """Get a RedisLite connection."""
    return RedisLiteConnection(tmp_path)
