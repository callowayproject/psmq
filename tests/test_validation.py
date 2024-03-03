"""Tests of the validation module."""

import pytest

from psmq.exceptions import InvalidCharacter, InvalidQueueName, QueueNameTooLong, ValueTooHigh, ValueTooLow
from psmq.validation import validate_int, validate_queue_name


class TestValidateQueueName:
    """Tests of the validate_queue_name function."""

    def test_valid_name_returns_true(self):
        assert validate_queue_name("valid_queue_name")

    def test_empty_queue_name_raises_error_if_requested(self):
        with pytest.raises(InvalidQueueName):
            validate_queue_name("", raise_on_error=True)

    def test_empty_queue_name_returns_false(self):
        assert not validate_queue_name("")

    def test_long_name_raises_error_when_requested(self):
        long_queue_name = "a" * 161  # Exceeds max length
        with pytest.raises(QueueNameTooLong):
            validate_queue_name(long_queue_name, raise_on_error=True)

    def test_long_name_returns_false(self):
        long_queue_name = "a" * 161  # Exceeds max length
        assert not validate_queue_name(long_queue_name)

    def test_name_with_invalid_chars_raises_error_when_requested(self):
        with pytest.raises(InvalidCharacter):
            validate_queue_name("invalid@name", raise_on_error=True)

    def test_name_with_invalid_chars_returns_false(self):
        assert not validate_queue_name("invalid@name")


class TestValidateInt:
    """Test the validate_int function."""

    def test_valid_int_returns_true(self):
        assert validate_int(10, min_value=5, max_value=15)

    def test_int_too_low_raises_error_if_requested(self):
        with pytest.raises(ValueTooLow):
            validate_int(3, min_value=5, raise_on_error=True)

    def test_int_too_low_returns_false(self):
        assert not validate_int(3, min_value=5)

    def test_int_too_high_raises_error_if_requested(self):
        with pytest.raises(ValueTooHigh):
            validate_int(16, max_value=15, raise_on_error=True)

    def test_int_too_high_returns_false(self):
        assert not validate_int(16, max_value=15)

    def test_non_int_value_value_raises_error_if_requested(self):
        with pytest.raises(TypeError):
            validate_int("not_an_int", raise_on_error=True)

    def test_non_int_value_returns_false(self):
        assert not validate_int("not_an_int")

    def test_valid_int_without_min_max_returns_true(self):
        assert validate_int(10)
        assert validate_int(10, raise_on_error=True)
