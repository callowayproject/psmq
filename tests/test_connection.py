"""Tests for `psmq` package."""
import datetime

import pytest
from psmq.connection import RedisLiteConnection


def test_create_queue(conn: RedisLiteConnection):
    """A queue is created."""
    r = conn.create_queue("test_queue", 10, 0, 0)
    assert r
    assert conn._connection.sismember("QUEUES", "test_queue")
    r = conn.create_queue("test_queue", 10, 0, 0)
    assert not r


def test_get_queue_info(conn: RedisLiteConnection):
    """We get information about a queue."""
    ts = int(datetime.datetime.now().timestamp())
    r = conn.get_queue_info("test_queue")

    assert r["config"].visibility_timeout == 60
    assert r["config"].initial_delay == 0
    assert r["config"].max_size == 65565
    assert r["metadata"].totalrecv == 0
    assert r["metadata"].totalsent == 0
    assert r["metadata"].created >= ts
    assert r["metadata"].modified >= ts
    assert r["metadata"].msgs == 0
    assert r["metadata"].hiddenmsgs == 0

    assert conn._connection.sismember("QUEUES", "test_queue")

    r = conn.create_queue("test_queue2", 10, 10, 10)
    assert r

    r = conn.get_queue_info("test_queue2")
    assert r["config"].visibility_timeout == 10
    assert r["config"].initial_delay == 10
    assert r["config"].max_size == 10


def test_set_queue_visibility_timeout(conn: RedisLiteConnection):
    """Can set the visibility timeout."""
    r = conn.create_queue("test_queue", 10, 10, 10)
    assert r
    conn.set_queue_visibility_timeout("test_queue", 20)
    r = conn.get_queue_info("test_queue")
    assert r["config"].visibility_timeout == 20


def test_set_queue_initial_delay(conn: RedisLiteConnection):
    """Can set the initial delay."""
    r = conn.create_queue("test_queue", 10, 10, 10)
    assert r
    conn.set_queue_initial_delay("test_queue", 20)
    r = conn.get_queue_info("test_queue")
    assert r["config"].initial_delay == 20


def test_set_queue_max_size(conn: RedisLiteConnection):
    """Can set the max size for a queue."""
    r = conn.create_queue("test_queue", 10, 10, 10)
    assert r
    conn.set_queue_max_size("test_queue", 20)
    r = conn.get_queue_info("test_queue")
    assert r["config"].max_size == 20


def test_push_message(conn: RedisLiteConnection):
    """You can send a message to an existing queue."""
    ts_msec = int((datetime.datetime.now().timestamp()) * 1_000)
    r = conn.create_queue("test_queue")
    assert r
    msg_id = conn.push_message("test_queue", "foo", 0)
    msg = conn._connection.hget("test_queue:Q", msg_id).decode("utf8")
    assert msg == "foo"

    messages = conn._connection.zrange("test_queue", 0, -1, withscores=True)
    assert len(messages) == 1
    assert messages[0][0].decode("utf8") == msg_id
    assert int(messages[0][1]) >= ts_msec


def test_push_message_missing_queue(conn: RedisLiteConnection):
    """You can send a message to a non-existing queue."""
    ts_msec = int((datetime.datetime.now().timestamp()) * 1_000)
    msg_id = conn.push_message("test_queue", "foo", 0)
    msg = conn._connection.hget("test_queue:Q", msg_id).decode("utf8")
    assert msg == "foo"


def test_push_message_initial_delay_override(conn: RedisLiteConnection):
    """You can send a message to a non-existing queue."""
    r = conn.create_queue("test_queue", delay=10)
    assert r
    info = conn.get_queue_info("test_queue")
    assert info["config"].initial_delay == 10

    # send a message at the default initial delay
    msg_id = conn.push_message("test_queue", "should be delayed")

    # send a message with an override initial delay
    msg_id2 = conn.push_message("test_queue", "should not be delayed", 0)

    messages = conn._connection.zrange("test_queue", 0, -1, withscores=True)
    assert len(messages) == 2

    import pprint

    pprint.pprint(messages)

    msg2 = messages[0]
    msg1 = messages[1]
    assert msg1[0].decode("utf8") == msg_id
    assert msg2[0].decode("utf8") == msg_id2
    assert int(msg1[1]) > int(msg2[1])


def test_get_message(conn: RedisLiteConnection):
    """You can get a message from an existing queue."""
    default_vt = 60

    msg_id = conn.push_message("test_queue", "foo")

    # Get the sorted messages before the get_message call
    pre_messages = conn._connection.zrange("test_queue", 0, -1, withscores=True)
    assert len(pre_messages) == 1

    # Get and verify the message
    msg = conn.get_message("test_queue")
    assert msg.message_id == msg_id
    assert msg.data == "foo"
    assert msg.retrieval_count == 1
    assert msg.sent < datetime.datetime.now()
    assert msg.first_retrieved < datetime.datetime.now()

    # Get the sorted messages after the get_message call
    post_messages = conn._connection.zrange("test_queue", 0, -1, withscores=True)
    assert len(post_messages) == 1

    # Verify the score was updated by the default_vt * 1,000
    sent_ts = int(pre_messages[0][1])
    delayed_ts = int(post_messages[0][1])
    assert delayed_ts - sent_ts == default_vt * 1_000

    # verify the queue stats
    queue_stats = conn._connection.hgetall("test_queue:Q")
    assert queue_stats[b"totalrecv"] == b"1"
    assert queue_stats[b"totalsent"] == b"1"


def test_get_message_override_vt(conn: RedisLiteConnection):
    """You can get a message from an existing queue and overrides the vt."""
    viz_timeout = 10
    msg_id = conn.push_message("test_queue", "foo")

    # Get the sorted messages before the get_message call
    pre_messages = conn._connection.zrange("test_queue", 0, -1, withscores=True)
    assert len(pre_messages) == 1

    # Get and verify the message
    msg = conn.get_message("test_queue", visibility_timeout=viz_timeout)

    # Get the sorted messages after the get_message call
    post_messages = conn._connection.zrange("test_queue", 0, -1, withscores=True)
    assert len(post_messages) == 1

    # Verify the score was updated by the viz_timeout * 1,000
    sent_ts = int(pre_messages[0][1])
    delayed_ts = int(post_messages[0][1])
    assert delayed_ts - sent_ts == viz_timeout * 1_000


def test_delete_message(conn: RedisLiteConnection):
    """Deleting a message should remove it from the queue."""
    msg_id = conn.push_message("test_queue", "foo")

    # Get the sorted messages before the get_message call
    pre_messages = conn._connection.zrange("test_queue", 0, -1, withscores=True)
    assert len(pre_messages) == 1

    # Delete the message
    conn.delete_message("test_queue", msg_id)

    # Get the sorted messages after the get_message call
    post_messages = conn._connection.zrange("test_queue", 0, -1, withscores=True)
    assert len(post_messages) == 0

    # verify the queue stats
    queue_stats = conn._connection.hgetall("test_queue:Q")
    assert queue_stats[b"totalrecv"] == b"0"
    assert queue_stats[b"totalsent"] == b"1"
    assert msg_id.encode("utf8") not in queue_stats


def test_delete_missing_message(conn: RedisLiteConnection):
    """Deleting a non-existing message should do nothing."""
    conn._connection.fcall("create_queue", 1, "test_queue")

    # Get the sorted messages before the get_message call
    pre_messages = conn._connection.zrange("test_queue", 0, -1, withscores=True)
    assert len(pre_messages) == 0

    # Delete the message
    conn.delete_message("test_queue", "foo")

    # Get the sorted messages after the get_message call
    post_messages = conn._connection.zrange("test_queue", 0, -1, withscores=True)
    assert len(post_messages) == 0

    # verify the queue stats
    queue_stats = conn._connection.hgetall("test_queue:Q")
    assert queue_stats[b"totalrecv"] == b"0"
    assert queue_stats[b"totalsent"] == b"0"


def test_pop_message(conn: RedisLiteConnection):
    """Popping a message should remove it from the queue."""
    msg_id = conn.push_message("test_queue", "foo")

    # Get the sorted messages before the get_message call
    pre_messages = conn._connection.zrange("test_queue", 0, -1, withscores=True)
    assert len(pre_messages) == 1

    # Get and verify the message
    msg = conn.pop_message("test_queue")
    assert msg["msg_id"] == msg_id
    assert msg["msg_body"] == "foo"
    assert msg["rc"] == 1

    # Get the sorted messages after the get_message call
    post_messages = conn._connection.zrange("test_queue", 0, -1, withscores=True)
    assert len(post_messages) == 0

    # verify the queue stats
    queue_stats = conn._connection.hgetall("test_queue:Q")
    assert queue_stats[b"totalrecv"] == b"1"
    assert queue_stats[b"totalsent"] == b"1"


def test_pop_message_on_empty_queue(conn: RedisLiteConnection):
    """Popping a message on an empty queue should do nothing."""
    # Get the sorted messages before the get_message call
    pre_messages = conn._connection.zrange("test_queue", 0, -1, withscores=True)
    assert len(pre_messages) == 0

    # Get and verify the message
    msg = conn.pop_message("test_queue")
    assert msg == {}
