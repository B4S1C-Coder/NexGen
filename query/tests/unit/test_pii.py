"""Unit tests for PIIMasker (pii.py).

All tests are pure Python — no infrastructure needed.
Each test verifies one specific PII pattern is correctly masked.
"""

from __future__ import annotations

import pytest

from src.pii import PIIMasker


@pytest.fixture
def masker() -> PIIMasker:
    return PIIMasker()


class TestIPv4Masking:

    def test_ipv4_is_masked(self, masker) -> None:
        hits = [{"message": "Login from 192.168.1.45"}]
        result = masker.mask(hits)
        assert "<IP_ADDRESS>" in result[0]["message"]
        assert "192.168.1.45" not in result[0]["message"]

    def test_multiple_ipv4_masked(self, masker) -> None:
        hits = [{"message": "From 10.0.0.1 to 10.0.0.2"}]
        result = masker.mask(hits)
        assert result[0]["message"].count("<IP_ADDRESS>") == 2


class TestEmailMasking:

    def test_email_is_masked(self, masker) -> None:
        hits = [{"message": "User john@company.com logged in"}]
        result = masker.mask(hits)
        assert "<EMAIL>" in result[0]["message"]
        assert "john@company.com" not in result[0]["message"]


class TestJWTMasking:

    def test_jwt_is_masked(self, masker) -> None:
        token = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abc123def"
        hits = [{"token": token}]
        result = masker.mask(hits)
        assert "<JWT_TOKEN>" in result[0]["token"]
        assert "eyJ" not in result[0]["token"]


class TestAWSKeyMasking:

    def test_aws_key_is_masked(self, masker) -> None:
        hits = [{"message": "Key AKIAIOSFODNN7EXAMPLE found"}]
        result = masker.mask(hits)
        assert "<AWS_KEY>" in result[0]["message"]
        assert "AKIA" not in result[0]["message"]


class TestHashMasking:

    def test_md5_is_masked(self, masker) -> None:
        md5 = "d41d8cd98f00b204e9800998ecf8427e"
        hits = [{"hash": md5}]
        result = masker.mask(hits)
        assert "<HASH>" in result[0]["hash"]

    def test_sha256_is_masked(self, masker) -> None:
        sha = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        hits = [{"hash": sha}]
        result = masker.mask(hits)
        assert "<HASH>" in result[0]["hash"]


class TestPhoneMasking:

    def test_phone_is_masked(self, masker) -> None:
        hits = [{"message": "Call +1-8005551234"}]
        result = masker.mask(hits)
        assert "<PHONE>" in result[0]["message"]


class TestNestedAndListHandling:

    def test_nested_dict_masked(self, masker) -> None:
        hits = [{"user": {"email": "test@example.com"}}]
        result = masker.mask(hits)
        assert "<EMAIL>" in result[0]["user"]["email"]

    def test_list_values_masked(self, masker) -> None:
        hits = [{"ips": ["192.168.1.1", "10.0.0.1"]}]
        result = masker.mask(hits)
        assert all(ip == "<IP_ADDRESS>" for ip in result[0]["ips"])

    def test_non_string_values_preserved(self, masker) -> None:
        hits = [{"status": 200, "active": True}]
        result = masker.mask(hits)
        assert result[0]["status"] == 200
        assert result[0]["active"] is True


class TestOriginalNotModified:

    def test_original_dict_unchanged(self, masker) -> None:
        original = [{"message": "User john@test.com"}]
        masker.mask(original)
        assert "john@test.com" in original[0]["message"]


class TestTraceIDPreserved:

    def test_trace_id_not_masked(self, masker) -> None:
        hits = [{"trace.id": "abc-123-xyz-456"}]
        result = masker.mask(hits)
        assert result[0]["trace.id"] == "abc-123-xyz-456"


class TestEmptyInput:

    def test_empty_list_returns_empty(self, masker) -> None:
        assert masker.mask([]) == []

    def test_empty_dict_returns_empty(self, masker) -> None:
        assert masker.mask([{}]) == [{}]