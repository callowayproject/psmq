"""Test the low-level queue functions in lua."""
import datetime
import pytest
import redis

from psmq.connection import RedisLiteConnection, list_to_dict
from tests.conftest import conn


def test_b36_encode(conn: RedisLiteConnection):
    """The b36encode function should properly encode numbers to base36."""
    r = conn._connection.fcall("b36encode", 1, "35")
    assert r.decode("utf8") == "Z"
    r = conn._connection.fcall("b36encode", 1, "36")
    assert r.decode("utf8") == "10"

    with pytest.raises(redis.ResponseError):
        conn._connection.fcall("b36encode", 1, "foo")

    with pytest.raises(redis.ResponseError):
        conn._connection.fcall("b36encode", 1, -1)


def test_b36_decode(conn: RedisLiteConnection):
    """The b36decode function should properly decode base36-encoded strings."""
    r = conn._connection.fcall("b36decode", 1, "Z")
    assert r == 35

    r = conn._connection.fcall("b36decode", 1, "10")
    assert r == 36

    r = conn._connection.fcall("b36decode", 1, 10)
    assert r == 36


def test_make_message_id(conn: RedisLiteConnection):
    """A message id should be sortable and unique."""
    ts_usec = int(datetime.datetime.now().timestamp() * 1_000_000)
    r = conn._connection.fcall("make_message_id", 1, ts_usec).decode("utf8")
    assert len(r) > 22
    ts_encoding = r[:-22]
    assert int(ts_encoding, 36) == ts_usec


def test_create_queue(conn: RedisLiteConnection):
    """A queue is created."""
    r = conn._connection.fcall("create_queue", 4, "test_queue", 10, 0, 0)
    assert r == 1
    assert conn._connection.sismember("QUEUES", "test_queue")
    r = conn._connection.fcall("create_queue", 4, "test_queue", 10, 0, 0)
    assert r == 0


def test_get_queue_info(conn: RedisLiteConnection):
    """We get information about a queue."""
    ts = int(datetime.datetime.now().timestamp())
    r = list_to_dict(conn._connection.fcall("get_queue_info", 1, "test_queue"))

    assert int(r["vt"]) == 60  # vt
    assert int(r["delay"]) == 0  # delay
    assert int(r["maxsize"]) == 65565  # maxsize
    assert int(r["created"]) >= ts  # created
    assert int(r["modified"]) >= ts  # modified
    assert int(r["totalrecv"]) == 0  # totalrecv
    assert int(r["totalsent"]) == 0  # totalsent
    assert int(r["msgs"]) == 0  # nummsgs
    assert int(r["hiddenmsgs"]) == 0  # hiddenmsgs
    assert conn._connection.sismember("QUEUES", "test_queue")

    r = conn._connection.fcall("create_queue", 4, "test_queue2", 10, 10, 10)
    assert r == 1
    r = list_to_dict(conn._connection.fcall("get_queue_info", 1, "test_queue2"))
    assert int(r["vt"]) == 10  # vt
    assert int(r["delay"]) == 10  # delay
    assert int(r["maxsize"]) == 10  # maxsize


def test_set_queue_vt(conn: RedisLiteConnection):
    """Can set the visibility timeout."""
    r = conn._connection.fcall("create_queue", 4, "test_queue", 10, 10, 10)
    assert r == 1
    conn._connection.fcall("set_queue_viz_timeout", 2, "test_queue", 20)
    r = list_to_dict(conn._connection.fcall("get_queue_info", 1, "test_queue"))
    assert int(r["vt"]) == 20  # vt


def test_set_queue_initial_delay(conn: RedisLiteConnection):
    """Can set the initial delay."""
    r = conn._connection.fcall("create_queue", 4, "test_queue", 10, 10, 10)
    assert r == 1
    conn._connection.fcall("set_queue_initial_delay", 2, "test_queue", 20)
    r = list_to_dict(conn._connection.fcall("get_queue_info", 1, "test_queue"))
    assert int(r["delay"]) == 20  # delay


def test_set_queue_max_size(conn: RedisLiteConnection):
    """Can set the max size for a queue."""
    r = conn._connection.fcall("create_queue", 4, "test_queue", 10, 10, 10)
    assert r == 1
    conn._connection.fcall("set_queue_max_size", 2, "test_queue", 20)
    r = list_to_dict(conn._connection.fcall("get_queue_info", 1, "test_queue"))
    assert int(r["maxsize"]) == 20  # maxsize


def test_create_queue_defaults(conn: RedisLiteConnection):
    """You can create a queue with default values."""
    r = conn._connection.fcall("create_queue", 1, "test_queue")
    assert r == 1
    assert conn._connection.sismember("QUEUES", "test_queue")
    r = conn._connection.hgetall("test_queue:Q")
    assert set(r.keys()) == {b"created", b"delay", b"maxsize", b"modified", b"totalrecv", b"totalsent", b"vt"}
    assert int(r[b"delay"]) == 0
    assert int(r[b"maxsize"]) == 65565
    assert int(r[b"vt"]) == 60


def test_push_message(conn: RedisLiteConnection):
    """You can send a message to an existing queue."""
    ts_msec = int((datetime.datetime.now().timestamp()) * 1_000)
    r = conn._connection.fcall("create_queue", 1, "test_queue")
    assert r == 1
    msg_id = conn._connection.fcall("push_message", 3, "test_queue", "foo", 0).decode("utf8")
    msg = conn._connection.hget("test_queue:Q", msg_id).decode("utf8")
    assert msg == "foo"
    messages = conn._connection.zrange("test_queue", 0, -1, withscores=True)
    assert len(messages) == 1
    assert messages[0][0].decode("utf8") == msg_id
    assert int(messages[0][1]) >= ts_msec


def test_push_message_overrides_delay(conn: RedisLiteConnection):
    """You can send a message to an existing queue and override its delay."""
    r = conn._connection.fcall("create_queue", 3, "test_queue", "", 10)
    assert r == 1
    msg_id1 = conn._connection.fcall("push_message", 2, "test_queue", "should be delayed").decode("utf8")
    msg_id2 = conn._connection.fcall("push_message", 3, "test_queue", "should not be delayed", 0).decode("utf8")

    messages = conn._connection.zrange("test_queue", 0, -1, withscores=True)
    assert len(messages) == 2
    msg2 = messages[0]
    msg1 = messages[1]
    assert msg1[0].decode("utf8") == msg_id1
    assert msg2[0].decode("utf8") == msg_id2
    assert int(msg1[1]) > int(msg2[1])


def test_push_message_missing_queue(conn: RedisLiteConnection):
    """You can send a message to a non-existing queue."""
    ts_msec = int((datetime.datetime.now().timestamp()) * 1_000)
    msg_id = conn._connection.fcall("push_message", 3, "test_queue", "foo", 0).decode("utf8")
    msg = conn._connection.hget("test_queue:Q", msg_id).decode("utf8")
    assert msg == "foo"

    messages = conn._connection.zrange("test_queue", 0, -1, withscores=True)
    assert len(messages) == 1
    assert messages[0][0].decode("utf8") == msg_id
    assert int(messages[0][1]) >= ts_msec


def test_get_message(conn: RedisLiteConnection):
    """You can get a message from an existing queue."""
    viz_timeout = 10
    msg_id = conn._connection.fcall("push_message", 3, "test_queue", "foo", 0).decode("utf8")

    # Get the sorted messages before the get_message call
    pre_messages = conn._connection.zrange("test_queue", 0, -1, withscores=True)
    assert len(pre_messages) == 1

    # Get and verify the message
    msg = list_to_dict(conn._connection.fcall("get_message", 2, "test_queue", viz_timeout))
    assert msg["msg_id"] == msg_id
    assert msg["msg_body"] == "foo"
    assert msg["rc"] == 1

    # Get the sorted messages after the get_message call
    post_messages = conn._connection.zrange("test_queue", 0, -1, withscores=True)
    assert len(post_messages) == 1

    # Verify the score was updated by the viz_timeout * 1,000
    sent_ts = int(pre_messages[0][1])
    delayed_ts = int(post_messages[0][1])
    assert delayed_ts - sent_ts == viz_timeout * 1_000

    # verify the queue stats
    queue_stats = conn._connection.hgetall("test_queue:Q")
    assert queue_stats[b"totalrecv"] == b"1"
    assert queue_stats[b"totalsent"] == b"1"


def test_get_message_no_message_on_queue(conn: RedisLiteConnection):
    """You can get a blank message from an existing queue when there is not message on the queue."""
    r = conn._connection.fcall("create_queue", 3, "test_queue", "", 10)

    # Get and verify the message
    msg = list_to_dict(conn._connection.fcall("get_message", 1, "test_queue"))
    assert msg == {}


def test_get_message_uses_default_vt(conn: RedisLiteConnection):
    """You can get a message from an existing queue and it uses the queue's default visibility timeout."""
    viz_timeout = 10
    conn._connection.fcall("create_queue", 4, "test_queue", viz_timeout, 0, 0)
    conn._connection.fcall("push_message", 2, "test_queue", "foo").decode("utf8")

    # Get the sorted messages before the get_message call
    pre_messages = conn._connection.zrange("test_queue", 0, -1, withscores=True)
    assert len(pre_messages) == 1

    # Get and verify the message
    conn._connection.fcall("get_message", 1, "test_queue")

    # Get the sorted messages after the get_message call
    post_messages = conn._connection.zrange("test_queue", 0, -1, withscores=True)
    assert len(post_messages) == 1

    # Verify the score was updated by the queue's viz_timeout * 1,000
    sent_ts = int(pre_messages[0][1])
    delayed_ts = int(post_messages[0][1])
    assert delayed_ts - sent_ts == viz_timeout * 1_000

    # verify the queue stats
    queue_stats = conn._connection.hgetall("test_queue:Q")
    assert queue_stats[b"totalrecv"] == b"1"
    assert queue_stats[b"totalsent"] == b"1"


def test_delete_message(conn: RedisLiteConnection):
    """Deleting a message should remove it from the queue."""
    msg_id = conn._connection.fcall("push_message", 3, "test_queue", "foo", 0).decode("utf8")

    # Get the sorted messages before the get_message call
    pre_messages = conn._connection.zrange("test_queue", 0, -1, withscores=True)
    assert len(pre_messages) == 1

    # Delete the message
    conn._connection.fcall("delete_message", 2, "test_queue", msg_id)

    # Get the sorted messages after the get_message call
    post_messages = conn._connection.zrange("test_queue", 0, -1, withscores=True)
    assert len(post_messages) == 0

    # verify the queue stats
    queue_stats = conn._connection.hgetall("test_queue:Q")
    assert queue_stats[b"totalrecv"] == b"0"
    assert queue_stats[b"totalsent"] == b"1"
    assert msg_id.encode("utf8") not in queue_stats
    assert f"{msg_id}:rc".encode("utf8") not in queue_stats
    assert f"{msg_id}:fr".encode("utf8") not in queue_stats
    assert f"{msg_id}:metadata".encode("utf8") not in queue_stats


def test_delete_missing_message(conn: RedisLiteConnection):
    """Deleting a non-existing message should do nothing."""
    conn._connection.fcall("create_queue", 1, "test_queue")

    # Get the sorted messages before the get_message call
    pre_messages = conn._connection.zrange("test_queue", 0, -1, withscores=True)
    assert len(pre_messages) == 0

    # Delete the message
    conn._connection.fcall("delete_message", 2, "test_queue", "foo")

    # Get the sorted messages after the get_message call
    post_messages = conn._connection.zrange("test_queue", 0, -1, withscores=True)
    assert len(post_messages) == 0

    # verify the queue stats
    queue_stats = conn._connection.hgetall("test_queue:Q")
    assert queue_stats[b"totalrecv"] == b"0"
    assert queue_stats[b"totalsent"] == b"0"


def test_pop_message(conn: RedisLiteConnection):
    """Popping a message should remove it from the queue."""
    msg_id = conn._connection.fcall("push_message", 3, "test_queue", "foo", 0).decode("utf8")

    # Get the sorted messages before the get_message call
    pre_messages = conn._connection.zrange("test_queue", 0, -1, withscores=True)
    assert len(pre_messages) == 1

    # Get and verify the message
    msg = list_to_dict(conn._connection.fcall("pop_message", 1, "test_queue"))
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


def test_pop_message_empty_queue(conn: RedisLiteConnection):
    """Popping a message on an empty queue should return an empty list."""
    conn._connection.fcall("create_queue", 1, "test_queue")

    # Get and verify the message
    msg = list_to_dict(conn._connection.fcall("pop_message", 1, "test_queue"))
    assert msg == {}
