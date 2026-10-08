"""
Unit tests for custom Kafka serializers.
"""

import json

import pytest

from producer import JsonSerializer, KeySerializer


@pytest.mark.unit
class TestJsonSerializer:
    def test_serialize_simple_dict(self):
        serializer = JsonSerializer()
        result = serializer.serialize("test-topic", None, {"key": "value"})
        assert result == b'{"key": "value"}'

    def test_serialize_with_unicode(self):
        serializer = JsonSerializer()
        result = serializer.serialize(
            "test-topic", None, {"title": "مقالات فارسی"}
        )
        # Should use ensure_ascii=False, so unicode chars are preserved
        assert "مقالات فارسی" in result.decode("utf-8")

    def test_serialize_nested_dict(self):
        serializer = JsonSerializer()
        data = {"outer": {"inner": [1, 2, 3]}}
        result = serializer.serialize("test-topic", None, data)
        assert json.loads(result) == data

    def test_serialize_returns_bytes(self):
        serializer = JsonSerializer()
        result = serializer.serialize("test-topic", None, {"x": 1})
        assert isinstance(result, bytes)


@pytest.mark.unit
class TestKeySerializer:
    def test_serialize_string_key(self):
        serializer = KeySerializer()
        result = serializer.serialize("test-topic", None, "Python")
        assert result == b"Python"

    def test_serialize_empty_string(self):
        serializer = KeySerializer()
        result = serializer.serialize("test-topic", None, "")
        assert result is None

    def test_serialize_none(self):
        serializer = KeySerializer()
        result = serializer.serialize("test-topic", None, None)
        assert result is None