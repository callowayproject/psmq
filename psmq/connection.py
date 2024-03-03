"""Connection managers for queues."""

from pathlib import Path

from redis import Redis

PSMQ_LIBRARY_FILE = Path(__file__).parent.joinpath("psmq_library.lua")


def setup_redis_connection(connection: Redis) -> Redis:
    """Load the PSMQ Lua library into the Redis connection."""
    psmq_library = PSMQ_LIBRARY_FILE.read_text()
    connection.function_load(psmq_library)  # type: ignore[attr-defined]
    return connection


def get_redis_from_url(url: str) -> Redis:
    """Instantiate this class using a redis:// URL."""
    connection = Redis.from_url(url)
    return setup_redis_connection(connection)


def get_redis_from_path(path: Path) -> Redis:
    """Instantiate this class using a path to a RedisLite file."""
    from redislite import Redis as RedisLite

    connection = RedisLite(path / "queue.rdb")
    return setup_redis_connection(connection)
