"""
Unit tests for the process_event function.
"""

import pytest

from producer import process_event


@pytest.mark.unit
class TestProcessEvent:
    def test_processes_valid_event(
        self, mock_kafka_producer, sample_sse_line, empty_stats
    ):
        """A valid SSE line should be sent to Kafka."""
        process_event(mock_kafka_producer, sample_sse_line, empty_stats)

        # Producer.send should have been called once
        assert mock_kafka_producer.send.call_count == 1

        # Stats should reflect the sent event
        assert empty_stats["sent"] == 1
        assert empty_stats["skipped"] == 0
        assert empty_stats["failed"] == 0

    def test_skips_non_data_lines(self, mock_kafka_producer, empty_stats):
        """Lines not starting with 'data: ' should be ignored."""
        line = b": this is a comment line"
        process_event(mock_kafka_producer, line, empty_stats)

        assert mock_kafka_producer.send.call_count == 0
        assert empty_stats["sent"] == 0

    def test_skips_malformed_json(self, mock_kafka_producer, empty_stats):
        """A line with invalid JSON should be counted as skipped."""
        line = b"data: {this is not valid json}"
        process_event(mock_kafka_producer, line, empty_stats)

        assert mock_kafka_producer.send.call_count == 0
        assert empty_stats["skipped"] == 1
        assert empty_stats["sent"] == 0

    def test_handles_send_failure(
        self, mock_kafka_producer, sample_sse_line, empty_stats
    ):
        """If producer.send raises, it should be counted as failed."""
        mock_kafka_producer.send.side_effect = Exception("Kafka down")
        process_event(mock_kafka_producer, sample_sse_line, empty_stats)

        assert empty_stats["failed"] == 1
        assert empty_stats["sent"] == 0

    def test_handles_future_get_timeout(
        self, mock_kafka_producer, sample_sse_line, empty_stats
    ):
        """If future.get() times out, count as failed."""
        mock_kafka_producer.send.return_value.get.side_effect = TimeoutError(
            "timed out"
        )
        process_event(mock_kafka_producer, sample_sse_line, empty_stats)

        assert empty_stats["failed"] == 1

    def test_uses_correct_topic(
        self, mock_kafka_producer, sample_sse_line, empty_stats
    ):
        """Verify the producer sends to the correct topic."""
        process_event(mock_kafka_producer, sample_sse_line, empty_stats)

        call_args = mock_kafka_producer.send.call_args
        # First positional arg should be the topic
        assert call_args[0][0] == "wiki-events"

    def test_sends_full_event_as_value(
        self, mock_kafka_producer, sample_sse_line, empty_stats
    ):
        """Verify the value sent is the full event dict."""
        process_event(mock_kafka_producer, sample_sse_line, empty_stats)

        call_kwargs = mock_kafka_producer.send.call_args.kwargs
        value = call_kwargs["value"]
        assert isinstance(value, dict)
        assert value["title"] == "Python (programming language)"
        assert value["user"] == "TestUser"