"""Fixtures for vector tests."""

from geopandas import GeoDataFrame
from numpy import dtype, int8, ones, zeros
from numpy.typing import NDArray
from pytest import fixture
from shapely.geometry import box

from sds_data_model.constants import BBOXES, CategoryLookups, Schema


@fixture
def expected_name() -> str:
    """Expected VectorLayer name."""
    return "ctry_21_gb_buc"


@fixture
def expected_vector_metadata() -> None:
    """Expected VectorLayer metadata."""
    return None


@fixture
def expected_schema() -> Schema:
    """Expected VectorLayer schema."""
    return {
        "OBJECTID": "int32",
        "CTRY21CD": "object",
        "CTRY21NM": "int8",
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
            -1: "No data",
            0: "England",
            1: "Scotland",
            2: "Wales",
        },
    }


@fixture
def gpdf() -> GeoDataFrame:
    """Dummy GeoDataFrame."""
    return GeoDataFrame(
        data={
            "category": ["A", "B", "C", None],
            "geometry": [box(*bbox) for bbox in BBOXES[0:4]],
        },
        crs="EPSG:27700",
    )


@fixture
def expected_HL_array() -> NDArray[int8]:
    """The expected array for grid cell HL."""
    return zeros(
        shape=(10_000, 10_000),
        dtype=dtype("int8"),
    )


@fixture
def expected_HM_array() -> NDArray[int8]:
    """The expected array for grid cell HM."""
    return ones(
        shape=(10_000, 10_000),
        dtype=dtype("int8"),
    )
