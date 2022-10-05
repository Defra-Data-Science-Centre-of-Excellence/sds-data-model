"""Tests for Vector module."""
from pathlib import Path

from pytest import fixture

from sds_data_model.constants import CategoryLookups, Schema
from sds_data_model.vector import TiledVectorLayer, VectorLayer


@fixture
def expected_name() -> str:
    """Expected VectorLayer name."""
    return "ctry_21_gb_buc"


@fixture
def expected_metadata() -> None:
    """Expected VectorLayer metadata."""
    return None


@fixture
def expected_schema() -> Schema:
    """Expected VectorLayer schema."""
    return {
        "OBJECTID": "int32",
        "CTRY21CD": "object",
        "CTRY21NM": "int16",
        "CTRY21NMW": "object",
        "BNG_E": "int32",
        "BNG_N": "int32",
        "LONG": "float64",
        "LAT": "float64",
        "GlobalID": "object",
        "SHAPE_Leng": "float64",
        "SHAPE_Area": "float64",
    }


@fixture
def expected_category_lookups() -> CategoryLookups:
    """Expected VectorLayer category lookups."""
    return {
        "CTRY21NM": {
            0: "England",
            1: "Scotland",
            2: "Wales",
        },
    }


def test_vector_layer_from_files(
    shared_datadir: Path,
    expected_name: str,
    expected_schema: Schema,
    expected_metadata: None,
    expected_category_lookups: CategoryLookups,
) -> None:
    """Reading Countries[...].zip returns a VectorLayer with expected values."""
    data_path = str(
        "/vsizip/" / shared_datadir / "Countries__December_2021__GB_BUC.zip"
    )
    received = VectorLayer.from_files(
        data_path=data_path,
        convert_to_categorical=["CTRY21NM"],
        name=expected_name,
    )
    assert received.name == expected_name
    assert received.metadata == expected_metadata
    assert received.schema == expected_schema
    assert received.category_lookups == expected_category_lookups


def test_tiled_vector_layer_from_files(
    shared_datadir: Path,
    expected_name: str,
    expected_schema: Schema,
    expected_metadata: None,
    expected_category_lookups: CategoryLookups,
) -> None:
    """Reading Countries[...].zip returns a TiledVectorLayer with expected values."""
    data_path = str(
        "/vsizip/" / shared_datadir / "Countries__December_2021__GB_BUC.zip"
    )
    received = TiledVectorLayer.from_files(
        data_path=data_path,
        convert_to_categorical=["CTRY21NM"],
        name=expected_name,
    )
    assert received.name == expected_name
    assert received.metadata == expected_metadata
    assert received.schema == expected_schema
    assert received.category_lookups == expected_category_lookups
