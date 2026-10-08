"""
Shared pytest fixtures for the streaming data pipeline tests.
"""

import json
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def sample_wikimedia_event() -> dict:
    """A realistic Wikimedia RecentChange event as a dictionary."""
    return {
        "id": 1234567890,
        "type": "edit",
        "namespace": 0,
        "title": "Python (programming language)",
        "page_id": 23862,
        "user": "TestUser",
        "bot": False,
        "minor": True,
        "patrolled": False,
        "comment": "Fixed typo in intro section",
        "timestamp": 1728304800,
        "wiki": "enwiki",
        "server_url": "https://en.wikipedia.org",
        "server_name": "en.wikipedia.org",
        "length": {"old": 5000, "new": 5050},
        "revision": {"old": 123456, "new": 123457},
        "meta": {
            "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "domain": "en.wikipedia.org",
            "dt": "2026-10-08T10:00:00Z",
        },
    }


@pytest.fixture
def sample_bot_event() -> dict:
    """A Wikimedia event from a bot user."""
    return {
        "id": 1111111111,
        "type": "edit",
        "title": "Category:Test",
        "user": "SomeBot",
        "bot": True,
        "timestamp": 1728304800,
        "wiki": "enwiki",
        "meta": {"id": "bot-uuid-1234"},
    }


@pytest.fixture
def sample_anonymous_event() -> dict:
    """A Wikimedia event from an anonymous user (IP address)."""
    return {
        "id": 2222222222,
        "type": "edit",
        "title": "Some Article",
        "user": "~2026-12345-67",
        "bot": False,
        "timestamp": 1728304800,
        "wiki": "fawiki",
        "meta": {"id": "anon-uuid-5678"},
    }


@pytest.fixture
def sample_sse_line(sample_wikimedia_event) -> bytes:
    """A single SSE line as it comes from Wikimedia."""
    json_str = json.dumps(sample_wikimedia_event)
    return f"data: {json_str}".encode("utf-8")


@pytest.fixture
def mock_kafka_producer() -> MagicMock:
    """A mock KafkaProducer that records sent messages."""
    producer = MagicMock()
    # Mock the .send() method to return a future-like object
    future_mock = MagicMock()
    future_mock.get.return_value = None
    producer.send.return_value = future_mock
    return producer


@pytest.fixture
def empty_stats() -> dict:
    """A fresh stats dictionary."""
    return {"sent": 0, "skipped": 0, "failed": 0}