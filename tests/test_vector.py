from pathlib import Path

from pytest import fixture
from numpy import array_equal, dtype, full, int16
from numpy.typing import NDArray

from sds_data_model.vector import VectorLayer, TiledVectorLayer
from sds_data_model.constants import Schema, CategoryLookups
from sds_data_model.table import TableLayer


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


def test_vector_layer_from_files(
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


def test_tiled_vector_layer_from_files(
    shared_datadir: Path,
    expected_name: str,
    expected_schema: Schema,
    expected_metadata: None,
    expected_category_lookups: CategoryLookups,
) -> None:
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
def expected_array() -> NDArray[int16]:
    """The expected array for grid cell HL."""
    return full(
        shape=(10_000, 10_000),
        fill_value=10,
        dtype=dtype("int16"),
    )


def test_join(
    shared_datadir: Path,
    expected_array: NDArray[int16],
) -> None:
    """Joining `title_ids` to `100km_grid`, then rasterising on `id`, returns the expected array."""
    tile_ids_data_path = shared_datadir / "tile_ids.csv"
    tile_ids = TableLayer.from_file(
        data_path=str(tile_ids_data_path),
        name="tile_ids",
    )

    grid_100km_data_path = shared_datadir / "os_bng_grids_100km_grid.gpkg.gz"
    grid_100km = VectorLayer.from_files(
        data_path=str(grid_100km_data_path),
        name="100km_grid",
    )

    tiles = grid_100km.to_tiles()

    joined = tiles.join(
        other=tile_ids.df,
        how="inner",
    )

    dataset = joined.to_dataset_as_raster(
        columns=["id"],
    )

    received_array = dataset["id"][range(0, 10_000), range(0, 10_000)].values

    assert array_equal(
        a1=received_array,
        a2=expected_array,
    )
