from pathlib import Path

from pytest import fixture

from sds_data_model.vector import VectorLayer
from sds_data_model.constants import Schema, CategoryLookups


@fixture
def expected_name() -> str:
    return "ctry_21_gb_buc"


@fixture
def expected_metadata() -> None:
    return None


@fixture
def expected_schema() -> Schema:
    return {
        "OBJECTID": "int32",
        "CTRY21CD": "object",
        "CTRY21NM": "uint8",
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
    return {
        "CTRY21NM": {
            1: "England",
            2: "Scotland",
            3: "Wales",
        },
    }


def test_from_files(
    shared_datadir: Path,
    expected_name: str,
    expected_schema: Schema,
    expected_metadata: None,
    expected_category_lookups: CategoryLookups,
) -> None:
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
