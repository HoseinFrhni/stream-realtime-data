"""
Lightweight integration tests for the pipeline logic.
These tests do NOT require Docker services to be running.
"""

import json

import pytest

from producer import extract_key, process_event


@pytest.mark.integration
class TestPipelineIntegration:
    def test_multiple_events_flow(self, mock_kafka_producer, empty_stats):
        """Simulate a small burst of Wikimedia events."""
        events = [
            {
                "id": i,
                "type": "edit",
                "title": f"Page {i}",
                "user": f"User{i}",
                "bot": i % 2 == 0,
                "timestamp": 1728304800 + i,
                "wiki": "enwiki",
                "meta": {"id": f"uuid-{i}"},
            }
            for i in range(10)
        ]

        for event in events:
            line = f"data: {json.dumps(event)}".encode("utf-8")
            process_event(mock_kafka_producer, line, empty_stats)

        assert empty_stats["sent"] == 10
        assert empty_stats["skipped"] == 0
        assert empty_stats["failed"] == 0
        assert mock_kafka_producer.send.call_count == 10

    def test_mixed_valid_and_invalid_events(
        self, mock_kafka_producer, empty_stats
    ):
        """Mix valid events with malformed lines."""
        lines = [
            b'data: {"id": 1, "title": "Page 1", "meta": {"id": "u1"}}',
            b": heartbeat",
            b"data: {invalid json}",
            b'data: {"id": 2, "title": "Page 2", "meta": {"id": "u2"}}',
            b"",
        ]

        for line in lines:
            process_event(mock_kafka_producer, line, empty_stats)

        assert empty_stats["sent"] == 2
        assert empty_stats["skipped"] == 1

    def test_event_key_ordering_property(self, sample_wikimedia_event):
        """Events from the same page should have the same key."""
        event1 = {**sample_wikimedia_event}
        event2 = {**sample_wikimedia_event, "timestamp": 1728304900}

        # Same meta.id => same key => same Kafka partition => order preserved
        assert extract_key(event1) == extract_key(event2)