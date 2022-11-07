"""Tests for Metadata module."""
from pathlib import Path

import pytest
from pytest import FixtureRequest, fixture

from sds_data_model.metadata import Metadata


@pytest.mark.parametrize(
    argnames=[
        "path",
    ],
    argvalues=[
        ("remote_url",),
        ("local_file_path",),
    ],
    ids=[
        "Remote url",
        "Local file path",
    ],
)
def test_from_file(
    request: FixtureRequest, path: str, expected_metadata: Metadata
) -> None:
    """Returns the expected `Metadata` object, given the path to a known XML file."""
    # Arrange
    metadata_path_string = request.getfixturevalue(path)
    # Act
    received = Metadata.from_file(metadata_path_string)
    # Assert
    assert received == expected_metadata


def test_from_file_without_title(
    datadir: Path, expected_without_title: Metadata
) -> None:
    """Returns the expected `Metadata` object, given the path to a known XML file."""
    # Arrange
    metadata_path_string = str(datadir / "ramsar_without_title.xml")
    # Act
    received = Metadata.from_file(metadata_path_string)
    # Assert
    assert received == expected_without_title


def test_from_file_without_keywords(
    datadir: Path, expected_without_keywords: Metadata
) -> None:
    """Returns the expected `Metadata` object, given the path to a known XML file."""
    # Arrange
    metadata_path_string = str(datadir / "ramsar_without_keywords.xml")
    # Act
    received = Metadata.from_file(metadata_path_string)
    # Assert
    assert received == expected_without_keywords
