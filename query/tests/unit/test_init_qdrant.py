"""Unit tests for the Qdrant init script (scripts/init_qdrant.py).

Tests verify collection creation logic using a mocked QdrantClient.
No real Qdrant connection required.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from scripts.init_qdrant import (
    FEW_SHOT_COLLECTION,
    EMBEDDING_DIMENSION,
    create_few_shot_collection,
    verify_collection,
)


def make_mock_client(existing_collections: list[str] | None = None) -> MagicMock:
    """Build a mock QdrantClient with controllable collection list.

    Args:
        existing_collections: Names of collections that already exist.

    Returns:
        MagicMock configured to behave like QdrantClient.
    """
    client = MagicMock()

    # MagicMock(name=...) sets the mock's internal display name, NOT a .name
    # attribute. Must create the object first then assign .name separately.
    collection_mocks = []
    for name in (existing_collections or []):
        mock_col = MagicMock()
        mock_col.name = name          # ← assign after creation
        collection_mocks.append(mock_col)

    client.get_collections.return_value.collections = collection_mocks

    # Mock get_collection() response for verify_collection
    mock_info = MagicMock()
    mock_info.config.params.vectors.size = EMBEDDING_DIMENSION
    mock_info.config.params.vectors.distance = "Cosine"
    mock_info.points_count = 0
    client.get_collection.return_value = mock_info

    return client


class TestCreateFewShotCollection:
    """Tests for create_few_shot_collection()."""

    def test_creates_collection_when_not_exists(self) -> None:
        """Must call create_collection when collection does not exist."""
        client = make_mock_client(existing_collections=[])
        result = create_few_shot_collection(client)
        assert result is True
        client.create_collection.assert_called_once()

    def test_skips_creation_when_already_exists(self) -> None:
        """Must skip create_collection when collection already exists."""
        client = make_mock_client(existing_collections=[FEW_SHOT_COLLECTION])
        result = create_few_shot_collection(client)
        assert result is False
        client.create_collection.assert_not_called()

    def test_creates_with_correct_collection_name(self) -> None:
        """Must create collection with the correct name."""
        client = make_mock_client(existing_collections=[])
        create_few_shot_collection(client)
        call_kwargs = client.create_collection.call_args.kwargs
        assert call_kwargs["collection_name"] == FEW_SHOT_COLLECTION

    def test_creates_with_correct_dimension(self) -> None:
        """Must create collection with 768-dimensional vectors."""
        client = make_mock_client(existing_collections=[])
        create_few_shot_collection(client)
        call_kwargs = client.create_collection.call_args.kwargs
        vectors_config = call_kwargs["vectors_config"]
        assert vectors_config.size == EMBEDDING_DIMENSION

    def test_dimension_is_768(self) -> None:
        """EMBEDDING_DIMENSION constant must be 768 for nomic-embed-text."""
        assert EMBEDDING_DIMENSION == 768


class TestVerifyCollection:
    """Tests for verify_collection()."""

    def test_calls_get_collection_with_correct_name(self) -> None:
        """verify_collection must query the correct collection name."""
        client = make_mock_client()
        verify_collection(client)
        client.get_collection.assert_called_once_with(FEW_SHOT_COLLECTION)