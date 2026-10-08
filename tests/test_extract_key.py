"""
Unit tests for the extract_key function.
"""

import pytest

from producer import extract_key


@pytest.mark.unit
class TestExtractKey:
    def test_prefers_meta_id(self, sample_wikimedia_event):
        """When meta.id exists, it should be used as the key."""
        result = extract_key(sample_wikimedia_event)
        assert result == "a1b2c3d4-e5f6-7890-abcd-ef1234567890"

    def test_falls_back_to_title(self):
        """When meta.id is missing, use title."""
        event = {"title": "Python", "id": 123}
        assert extract_key(event) == "Python"

    def test_falls_back_to_id(self):
        """When meta.id and title are missing, use id."""
        event = {"id": 456}
        assert extract_key(event) == "456"

    def test_returns_empty_string_when_no_fields(self):
        """When none of the fields exist, return empty string."""
        assert extract_key({}) == ""

    def test_handles_meta_without_id(self):
        """When meta exists but has no id, fall back to title."""
        event = {"title": "Test Page", "meta": {"domain": "en.wikipedia.org"}}
        assert extract_key(event) == "Test Page"

    def test_handles_meta_not_a_dict(self):
        """When meta is not a dict, fall back to title."""
        event = {"title": "Test", "meta": "not-a-dict"}
        assert extract_key(event) == "Test"

    def test_handles_empty_meta_id(self):
        """When meta.id is empty string, fall back to title."""
        event = {"title": "Test", "meta": {"id": ""}}
        assert extract_key(event) == "Test"

    def test_converts_numeric_id_to_string(self):
        """Ensure id is always returned as a string."""
        event = {"id": 9999999999}
        assert extract_key(event) == "9999999999"
        assert isinstance(extract_key(event), str)

    def test_bot_event(self, sample_bot_event):
        result = extract_key(sample_bot_event)
        assert result == "bot-uuid-1234"

    def test_anonymous_event(self, sample_anonymous_event):
        result = extract_key(sample_anonymous_event)
        assert result == "anon-uuid-5678"