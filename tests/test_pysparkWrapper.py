"""Tests for Pyspark Wrapper."""
from pathlib import Path
from pytest import fixture

from sds_data_model.constants import CategoryLookups, Schema
from sds_data_model.metadata import Wrapper

from pyspark_vector_files import read_vector_files

data_path = "data/Countries_(December_2021)_GB_BUC"

expected_data = read_vector_files(
            path=data_path,
    suffix = ".shp"
        ).to_pandas_on_spark()

@fixture
def expected_name() -> str:
    """Expected Wrapper name."""
    return "ctry_21_gb_buc"

@fixture
def expected_metadata() -> None:
    """Expected Wrapper metadata."""
    return None

@fixture
def expected_schema() -> Schema:
    """Expected Wrapper schema."""
    return expected_data.spark.print_schema()

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



def test_from_files(
    shared_datadir: Path,
    expected_name: str,
    expected_schema: Schema,
    expected_metadata: None,
    expected_category_lookups: CategoryLookups,
) -> None:
    """Reading Countries returns a wrapper with expected values."""

    received = Wrapper.from_files(
        data_path=data_path,
        convert_to_categorical=["CTRY21NM"],
        suffix = '.shp',
        name=expected_name,
    )
    assert received.name == expected_name
    assert received.metadata == expected_metadata
    assert received.spark.print_schema == expected_schema
    assert received.category_lookups == expected_category_lookups


