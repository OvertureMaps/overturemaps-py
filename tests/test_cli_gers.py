"""Tests for GERS CLI argument validation."""

import click
import pytest

from overturemaps.cli import validate_gers_id


def test_validate_gers_id_valid_uuid():
    result = validate_gers_id(None, None, "0b7fc702-5b1c-4bf2-abc1-b8f3b6ae2e76")
    assert result == "0b7fc702-5b1c-4bf2-abc1-b8f3b6ae2e76"


def test_validate_gers_id_normalizes_uppercase():
    """uuid.UUID normalizes uppercase input to lowercase."""
    result = validate_gers_id(None, None, "0B7FC702-5B1C-4BF2-ABC1-B8F3B6AE2E76")
    assert result == "0b7fc702-5b1c-4bf2-abc1-b8f3b6ae2e76"


def test_validate_gers_id_empty_raises():
    with pytest.raises(click.BadParameter, match="cannot be empty"):
        validate_gers_id(None, None, "")


def test_validate_gers_id_not_uuid_raises():
    with pytest.raises(click.BadParameter, match="valid UUID"):
        validate_gers_id(None, None, "not-a-uuid")


def test_validate_gers_id_partial_uuid_raises():
    with pytest.raises(click.BadParameter, match="valid UUID"):
        validate_gers_id(None, None, "0b7fc702-5b1c")
