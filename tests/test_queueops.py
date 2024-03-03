"""Tests for `psmq` package."""

import datetime

import pytest
from redis.client import Redis
from psmq import queue_ops
from msgpack import unpackb


def test_list_queues_returns_a_set_of_queue_names(conn: Redis):
    """We can list queues."""
    assert queue_ops.list_queues(conn) == set()

    conn.fcall("create_queue", 1, "test_queue")
    assert queue_ops.list_queues(conn) == {"test_queue"}

    conn.fcall("create_queue", 1, "test_queue2")
    assert queue_ops.list_queues(conn) == {"test_queue", "test_queue2"}


class TestCreateQueue:
    """Tests for creating and deleting queues."""

    def test_creates_a_queue(self, conn: Redis):
        """A queue is created."""
        r = queue_ops.create_queue(conn, "test_queue", 10, 0, 0)
        assert r
        assert conn.sismember("QUEUES", "test_queue")

    def test_is_idempotent(self, conn: Redis):
        r = queue_ops.create_queue(conn, "test_queue", 10, 0, 0)
        assert r
        assert conn.sismember("QUEUES", "test_queue")
        r = queue_ops.create_queue(conn, "test_queue", 10, 0, 0)
        assert not r


class TestDeleteQueue:
    """Tests for deleting queues."""

    def test_deletes_a_queue(self, conn: Redis):
        """A queue is deleted."""
        r = queue_ops.create_queue(conn, "test_queue", 10, 0, 0)
        assert r
        assert conn.sismember("QUEUES", "test_queue")
        queue_ops.delete_queue(conn, "test_queue")
        assert not conn.sismember("QUEUES", "test_queue")

    def test_is_idempotent(self, conn: Redis):
        """A queue is deleted."""
        assert not conn.sismember("QUEUES", "test_queue")
        queue_ops.delete_queue(conn, "test_queue")
        assert not conn.sismember("QUEUES", "test_queue")


def test_get_queue_info_returns_a_dict_of_metadata(conn: Redis):
    """We get information about a queue."""
    ts = int(datetime.datetime.now().timestamp())
    r = queue_ops.get_queue_info(conn, "test_queue")

    assert r["config"].visibility_timeout == 60
    assert r["config"].initial_delay == 0
    assert r["config"].max_size == 65565
    assert r["metadata"].totalrecv == 0
    assert r["metadata"].totalsent == 0
    assert r["metadata"].created >= ts
    assert r["metadata"].modified >= ts
    assert r["metadata"].msgs == 0
    assert r["metadata"].hiddenmsgs == 0

    assert conn.sismember("QUEUES", "test_queue")

    r = queue_ops.create_queue(conn, "test_queue2", 10, 10, 10)
    assert r

    r = queue_ops.get_queue_info(conn, "test_queue2")
    assert r["config"].visibility_timeout == 10
    assert r["config"].initial_delay == 10
    assert r["config"].max_size == 10


def test_set_queue_visibility_timeout_changes_the_value(conn: Redis):
    """Can set the visibility timeout."""
    r = queue_ops.create_queue(conn, "test_queue", 10, 10, 10)
    assert r
    queue_ops.set_queue_visibility_timeout(conn, "test_queue", 20)
    r = queue_ops.get_queue_info(conn, "test_queue")
    assert r["config"].visibility_timeout == 20


def test_set_queue_initial_delay_changes_value(conn: Redis):
    """Can set the initial delay."""
    r = queue_ops.create_queue(conn, "test_queue", 10, 10, 10)
    assert r
    queue_ops.set_queue_initial_delay(conn, "test_queue", 20)
    r = queue_ops.get_queue_info(conn, "test_queue")
    assert r["config"].initial_delay == 20


def test_set_queue_max_size_changes_value(conn: Redis):
    """Can set the max size for a queue."""
    r = queue_ops.create_queue(conn, "test_queue", 10, 10, 10)
    assert r
    queue_ops.set_queue_max_size(conn, "test_queue", 20)
    r = queue_ops.get_queue_info(conn, "test_queue")
    assert r["config"].max_size == 20


class TestPushMessage:
    """Tests for sending messages."""

    def test_can_send_to_an_existing_queue(self, conn: Redis):
        """You can send a message to an existing queue."""
        ts_msec = int((datetime.datetime.now().timestamp()) * 1_000)
        r = queue_ops.create_queue(conn, "test_queue")
        assert r
        msg_id = queue_ops.push_message(conn, "test_queue", "foo".encode("utf-8"), 0)
        msg = conn.hget("test_queue:Q", msg_id).decode("utf8")
        assert msg == "foo"

        messages = conn.zrange("test_queue", 0, -1, withscores=True)
        assert len(messages) == 1
        assert messages[0][0].decode("utf8") == msg_id
        assert int(messages[0][1]) >= ts_msec

    def test_can_send_to_a_nonexisting_queue(self, conn: Redis):
        """You can send a message to a non-existing queue."""
        ts_msec = int((datetime.datetime.now().timestamp()) * 1_000)
        msg_id = queue_ops.push_message(conn, "test_queue", "foo".encode("utf-8"), 0)
        msg = conn.hget("test_queue:Q", msg_id).decode("utf8")
        assert msg == "foo"

    def test_can_override_the_initial_delay(self, conn: Redis):
        """You can send a message with an overridden initial delay."""
        r = queue_ops.create_queue(conn, "test_queue", delay=10)
        assert r
        info = queue_ops.get_queue_info(conn, "test_queue")
        assert info["config"].initial_delay == 10

        # send a message at the default initial delay
        msg_id = queue_ops.push_message(conn, "test_queue", "should be delayed".encode("utf-8"))

        # send a message with an override initial delay
        msg_id2 = queue_ops.push_message(conn, "test_queue", "should not be delayed".encode("utf-8"), 0)

        messages = conn.zrange("test_queue", 0, -1, withscores=True)
        assert len(messages) == 2

        msg2 = messages[0]
        msg1 = messages[1]
        assert msg1[0].decode("utf8") == msg_id
        assert msg2[0].decode("utf8") == msg_id2
        assert int(msg1[1]) > int(msg2[1])

    def test_can_send_metadata_with_message(self, conn: Redis):
        """You can send a message with metadata."""
        msg_id = queue_ops.push_message(conn, "test_queue", "foo".encode("utf-8"), metadata={"foo": "bar"})

        # metadata is stored as a msgpack blob
        msg_metadata = unpackb(conn.hget("test_queue:Q", f"{msg_id}:metadata"))

        assert msg_metadata["foo"] == "bar"
        assert "sent" in msg_metadata

    def test_raises_error_if_metadata_is_not_dict(self, conn: Redis):
        """You can send a message with metadata."""
        with pytest.raises(TypeError):
            queue_ops.push_message(conn, "test_queue", "foo".encode("utf-8"), metadata="foo")


class TestGetMessage:
    """Tests for getting messages."""

    def test_can_get_a_message_from_an_existing_queue(self, conn: Redis):
        """You can get a message from an existing queue."""
        ts_msec = int((datetime.datetime.now().timestamp()) * 1_000)
        r = queue_ops.create_queue(conn, "test_queue")
        assert r
        msg_id = queue_ops.push_message(conn, "test_queue", "foo".encode("utf-8"), 0)
        msg = queue_ops.get_message(conn, "test_queue")
        assert msg.message_id == msg_id
        assert msg.data == "foo"
        assert msg.retrieval_count == 1
        assert msg.sent < datetime.datetime.now()
        assert msg.first_retrieved < datetime.datetime.now()
        assert "sent" in msg.metadata

        messages = conn.zrange("test_queue", 0, -1, withscores=True)
        assert len(messages) == 1
        assert messages[0][0].decode("utf8") == msg_id
        assert int(messages[0][1]) >= ts_msec

    def test_getting_a_message_from_a_nonexisting_queue_returns_none(self, conn: Redis):
        """You can get a message from a non-existing queue, but it is empty."""
        msg = queue_ops.get_message(conn, "test_queue")
        assert msg is None

    def test_can_override_vt(self, conn: Redis):
        """You can get a message from an existing queue and overrides the vt."""
        viz_timeout = 10
        msg_id = queue_ops.push_message(conn, "test_queue", "foo".encode("utf-8"))

        # Get the sorted messages before the get_message call
        pre_messages = conn.zrange("test_queue", 0, -1, withscores=True)
        assert len(pre_messages) == 1

        # Get and verify the message
        msg = queue_ops.get_message(conn, "test_queue", visibility_timeout=viz_timeout)

        # Get the sorted messages after the get_message call
        post_messages = conn.zrange("test_queue", 0, -1, withscores=True)
        assert len(post_messages) == 1

        # Verify the score was updated by the viz_timeout * 1,000
        sent_ts = int(pre_messages[0][1])
        delayed_ts = int(post_messages[0][1])
        assert delayed_ts - sent_ts == viz_timeout * 1_000


class TestDeleteMessage:
    """Tests for deleting messages."""

    def test_delete_message(self, conn: Redis):
        """Deleting a message should remove it from the queue."""
        msg_id = queue_ops.push_message(conn, "test_queue", "foo".encode("utf-8"))

        # Get the sorted messages before the get_message call
        pre_messages = conn.zrange("test_queue", 0, -1, withscores=True)
        assert len(pre_messages) == 1

        # Delete the message
        queue_ops.delete_message(conn, "test_queue", msg_id)

        # Get the sorted messages after the get_message call
        post_messages = conn.zrange("test_queue", 0, -1, withscores=True)
        assert len(post_messages) == 0

        # verify the queue stats
        queue_stats = conn.hgetall("test_queue:Q")
        assert queue_stats[b"totalrecv"] == b"0"
        assert queue_stats[b"totalsent"] == b"1"
        assert msg_id.encode("utf8") not in queue_stats

    def test_is_idempotent(self, conn: Redis):
        """Deleting a non-existing message should do nothing."""
        conn.fcall("create_queue", 1, "test_queue")

        # Get the sorted messages before the get_message call
        pre_messages = conn.zrange("test_queue", 0, -1, withscores=True)
        assert len(pre_messages) == 0

        # Delete the message
        queue_ops.delete_message(conn, "test_queue", "foo")

        # Get the sorted messages after the get_message call
        post_messages = conn.zrange("test_queue", 0, -1, withscores=True)
        assert len(post_messages) == 0

        # verify the queue stats
        queue_stats = conn.hgetall("test_queue:Q")
        assert queue_stats[b"totalrecv"] == b"0"
        assert queue_stats[b"totalsent"] == b"0"


class TestPopMessage:
    """Tests for popping messages."""

    def test_pop_message(self, conn: Redis):
        """Popping a message should remove it from the queue."""
        msg_id = queue_ops.push_message(conn, "test_queue", "foo".encode("utf-8"))

        # Get the sorted messages before the get_message call
        pre_messages = conn.zrange("test_queue", 0, -1, withscores=True)
        assert len(pre_messages) == 1

        # Get and verify the message
        msg = queue_ops.pop_message(conn, "test_queue")
        assert msg["msg_id"] == msg_id
        assert msg["msg_body"] == "foo"
        assert msg["rc"] == 1

        # Get the sorted messages after the get_message call
        post_messages = conn.zrange("test_queue", 0, -1, withscores=True)
        assert len(post_messages) == 0

        # verify the queue stats
        queue_stats = conn.hgetall("test_queue:Q")
        assert queue_stats[b"totalrecv"] == b"1"
        assert queue_stats[b"totalsent"] == b"1"

    def test_pop_message_on_empty_queue_does_nothing(self, conn: Redis):
        """Popping a message on an empty queue should do nothing."""
        # Get the sorted messages before the get_message call
        pre_messages = conn.zrange("test_queue", 0, -1, withscores=True)
        assert len(pre_messages) == 0

        # Get and verify the message
        msg = queue_ops.pop_message(conn, "test_queue")
        assert msg == {}
