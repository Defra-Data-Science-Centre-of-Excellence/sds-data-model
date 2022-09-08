"""Tests for Vector module."""
from pathlib import Path

from dask.diagnostics import ProgressBar
from geopandas import GeoDataFrame
from numpy import array_equal, dtype, int8, ones, zeros
from numpy.typing import NDArray
from pytest import fixture
from shapely.geometry import box
from xarray import open_dataset

from sds_data_model.constants import BBOXES, CategoryLookups, Schema
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


@fixture
def gpdf() -> GeoDataFrame:
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


def test_pipeline(
    gpdf: GeoDataFrame,
    tmp_path: Path,
    expected_HL_array: NDArray[int8],
    expected_HM_array: NDArray[int8],
) -> None:
    gpkg_path = tmp_path / "test.gpkg"
    zarr_path = tmp_path / "test.zarr"

    gpdf.to_file(gpkg_path)

    pipeline = (
        VectorLayer.from_files(
            data_path=gpkg_path,
            name="test",
            convert_to_categorical=["category"],
        )
        .to_tiles()
        .to_dataset_as_raster(columns=["category"])
        .to_zarr(
            store=zarr_path,
            mode="w",
            compute=False,
        )
    )
    with ProgressBar():
        pipeline.compute()

    dataset = open_dataset(
        zarr_path,
        chunks={
            "northings": 10_000,
            "eastings": 10_000,
        },
        engine="zarr",
    )

    received_HL_array = dataset["category"][range(0, 10_000), range(0, 10_000)].values

    assert array_equal(
        a1=received_HL_array,
        a2=expected_HL_array,
    )

    received_HM_array = dataset["category"][
        range(0, 10_000), range(10_000, 20_000)
    ].values

    assert array_equal(
        a1=received_HM_array,
        a2=expected_HM_array,
    )
