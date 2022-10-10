"""Tests for raster module."""
from pathlib import Path

import pytest
import rioxarray  # noqa: F401 - Needed for `.rio` accessor methods
from numpy import arange, array, ones
from pytest import FixtureRequest, fixture
from xarray import DataArray, Dataset
from xarray.testing import assert_identical

from sds_data_model._raster import _read_dataset_from_file


@fixture
def same_cell_size_same_shape_dataset() -> Dataset:
    """A dataset with the same cell size and same shape."""
    coords = {
        "northings": arange(5.5, 0, -1),
        "eastings": arange(0.5, 6, 1),
    }

    ones_data = ones(shape=(6, 6), dtype="uint8")

    ones_data_array = DataArray(
        data=ones_data,
        coords=coords,
        name="ones",
    )

    numbers_data = array(
        [
            [0, 0, 1, 1, 2, 2],
            [0, 0, 1, 1, 2, 2],
            [3, 3, 4, 4, 5, 5],
            [3, 3, 4, 4, 5, 5],
            [6, 6, 7, 7, 8, 8],
            [6, 6, 7, 7, 8, 8],
        ]
    ).astype("uint8")

    numbers_data_array = DataArray(
        data=numbers_data,
        coords=coords,
        name="numbers",
    )

    dataset = Dataset(
        data_vars={
            "ones": ones_data_array,
            "numbers": numbers_data_array,
        },
        coords=coords,
    )

    (
        dataset.rio.set_spatial_dims(
            x_dim="eastings",
            y_dim="northings",
            inplace=True,
        ).rio.write_crs(
            27700,
            inplace=True,
        )
    )
    return dataset


@fixture
def same_cell_size_same_shape(
    same_cell_size_same_shape_dataset: Dataset,
    shared_datadir: Path,
    name: str = "same_cell_size_same_shape",
) -> str:
    """Write a dataset with the same cell size and same shape."""
    data_path = shared_datadir / f"{name}.tif"

    same_cell_size_same_shape_dataset.rio.to_raster(data_path)

    return str(data_path)


@fixture
def larger_cell_size_same_shape(
    shared_datadir: Path,
    name: str = "larger_cell_size_same_shape",
) -> str:
    """A dataset with a larger cell size and same spatial shape."""
    coords = {
        "northings": arange(5, 0, -2),
        "eastings": arange(1, 6, 2),
    }

    ones_data = ones(shape=(3, 3), dtype="uint8")

    ones_data_array = DataArray(
        data=ones_data,
        coords=coords,
        name="ones",
    )

    numbers_data = array(
        [
            [0, 1, 2],
            [3, 4, 5],
            [6, 7, 8],
        ]
    ).astype("uint8")

    numbers_data_array = DataArray(
        data=numbers_data,
        coords=coords,
        name="numbers",
    )

    dataset = Dataset(
        data_vars={
            "ones": ones_data_array,
            "numbers": numbers_data_array,
        },
        coords=coords,
    )

    (
        dataset.rio.set_spatial_dims(
            x_dim="eastings",
            y_dim="northings",
            inplace=True,
        ).rio.write_crs(
            27700,
            inplace=True,
        )
    )

    data_path = shared_datadir / f"{name}.tif"

    dataset.rio.to_raster(data_path)

    return str(data_path)


@fixture
def smaller_cell_size_same_shape(
    shared_datadir: Path,
    name: str = "smaller_cell_size_same_shape",
) -> str:
    """A dataset with a smaller cell size and same spatial shape."""
    coords = {
        "northings": arange(5.75, 0, -0.5),
        "eastings": arange(0.25, 6, 0.5),
    }

    ones_data = ones(shape=(12, 12), dtype="uint8")

    ones_data_array = DataArray(
        data=ones_data,
        coords=coords,
        name="ones",
    )

    numbers_data = array(
        [
            [0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2],
            [0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2],
            [0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2],
            [0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2],
            [3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5],
            [3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5],
            [3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5],
            [3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5],
            [6, 6, 6, 6, 7, 7, 7, 7, 8, 8, 8, 8],
            [6, 6, 6, 6, 7, 7, 7, 7, 8, 8, 8, 8],
            [6, 6, 6, 6, 7, 7, 7, 7, 8, 8, 8, 8],
            [6, 6, 6, 6, 7, 7, 7, 7, 8, 8, 8, 8],
        ]
    ).astype("uint8")

    numbers_data_array = DataArray(
        data=numbers_data,
        coords=coords,
        name="numbers",
    )

    dataset = Dataset(
        data_vars={
            "ones": ones_data_array,
            "numbers": numbers_data_array,
        },
        coords=coords,
    )

    (
        dataset.rio.set_spatial_dims(
            x_dim="eastings",
            y_dim="northings",
            inplace=True,
        ).rio.write_crs(
            27700,
            inplace=True,
        )
    )

    data_path = shared_datadir / f"{name}.tif"

    dataset.rio.to_raster(data_path)

    return str(data_path)


@fixture
def same_cell_size_same_shape_dataset_with_nodata() -> Dataset:
    """A dataset with the same cell size and same shape but with nodata."""
    coords = {
        "northings": arange(5.5, 0, -1),
        "eastings": arange(0.5, 6, 1),
    }

    ones_data = array(
        [
            [0, 0, 0, 0, 0, 0],
            [0, 0, 0, 0, 0, 0],
            [0, 0, 1, 1, 1, 1],
            [0, 0, 1, 1, 1, 1],
            [0, 0, 1, 1, 1, 1],
            [0, 0, 1, 1, 1, 1],
        ]
    ).astype("uint8")

    ones_data_array = DataArray(
        data=ones_data,
        coords=coords,
        name="ones",
    )

    numbers_data = array(
        [
            [0, 0, 0, 0, 0, 0],
            [0, 0, 0, 0, 0, 0],
            [0, 0, 4, 4, 5, 5],
            [0, 0, 4, 4, 5, 5],
            [0, 0, 7, 7, 8, 8],
            [0, 0, 7, 7, 8, 8],
        ]
    ).astype("uint8")

    numbers_data_array = DataArray(
        data=numbers_data,
        coords=coords,
        name="numbers",
    )

    dataset = Dataset(
        data_vars={
            "ones": ones_data_array,
            "numbers": numbers_data_array,
        },
        coords=coords,
    )

    (
        dataset.rio.set_spatial_dims(
            x_dim="eastings",
            y_dim="northings",
            inplace=True,
        ).rio.write_crs(
            27700,
            inplace=True,
        )
    )

    return dataset


@fixture
def same_cell_size_smaller_shape(
    shared_datadir: Path,
    name: str = "same_cell_size_smaller_shape",
) -> str:
    """A dataset with the same cell size but a smaller shape."""
    coords = {
        "northings": arange(3.5, 0, -1),
        "eastings": arange(2.5, 6, 1),
    }

    ones_data = ones(shape=(4, 4), dtype="uint8")

    ones_data_array = DataArray(
        data=ones_data,
        coords=coords,
        name="ones",
    )

    numbers_data = array(
        [
            [4, 4, 5, 5],
            [4, 4, 5, 5],
            [7, 7, 8, 8],
            [7, 7, 8, 8],
        ]
    ).astype("uint8")

    numbers_data_array = DataArray(
        data=numbers_data,
        coords=coords,
        name="numbers",
    )

    dataset = Dataset(
        data_vars={
            "ones": ones_data_array,
            "numbers": numbers_data_array,
        },
        coords=coords,
    )

    (
        dataset.rio.set_spatial_dims(
            x_dim="eastings",
            y_dim="northings",
            inplace=True,
        ).rio.write_crs(
            27700,
            inplace=True,
        )
    )

    data_path = shared_datadir / f"{name}.tif"

    dataset.rio.to_raster(data_path)

    return str(data_path)


@fixture
def larger_cell_size_smaller_shape(
    shared_datadir: Path,
    name: str = "larger_cell_size_smaller_shape",
) -> str:
    """A dataset with a larger cell size and a smaller shape."""
    coords = {
        "northings": arange(3.5, 0, -2),
        "eastings": arange(2.5, 6, 2),
    }

    ones_data = ones(shape=(2, 2), dtype="uint8")

    ones_data_array = DataArray(
        data=ones_data,
        coords=coords,
        name="ones",
    )

    numbers_data = array(
        [
            [4, 5],
            [7, 8],
        ]
    ).astype("uint8")

    numbers_data_array = DataArray(
        data=numbers_data,
        coords=coords,
        name="numbers",
    )

    dataset = Dataset(
        data_vars={
            "ones": ones_data_array,
            "numbers": numbers_data_array,
        },
        coords=coords,
    )

    (
        dataset.rio.set_spatial_dims(
            x_dim="eastings",
            y_dim="northings",
            inplace=True,
        ).rio.write_crs(
            27700,
            inplace=True,
        )
    )

    data_path = shared_datadir / f"{name}.tif"

    dataset.rio.to_raster(data_path)

    return str(data_path)


@fixture
def smaller_cell_size_smaller_shape(
    shared_datadir: Path,
    name: str = "smaller_cell_size_smaller_shape",
) -> str:
    """A dataset with a smaller cell size and a smaller shape."""
    coords = {
        "northings": arange(3.75, 0, -0.5),
        "eastings": arange(2.25, 6, 0.5),
    }

    ones_data = ones(shape=(8, 8), dtype="uint8")

    ones_data_array = DataArray(
        data=ones_data,
        coords=coords,
        name="ones",
    )

    numbers_data = array(
        [
            [4, 4, 4, 4, 5, 5, 5, 5],
            [4, 4, 4, 4, 5, 5, 5, 5],
            [4, 4, 4, 4, 5, 5, 5, 5],
            [4, 4, 4, 4, 5, 5, 5, 5],
            [7, 7, 7, 7, 8, 8, 8, 8],
            [7, 7, 7, 7, 8, 8, 8, 8],
            [7, 7, 7, 7, 8, 8, 8, 8],
            [7, 7, 7, 7, 8, 8, 8, 8],
        ]
    ).astype("uint8")

    numbers_data_array = DataArray(
        data=numbers_data,
        coords=coords,
        name="numbers",
    )

    dataset = Dataset(
        data_vars={
            "ones": ones_data_array,
            "numbers": numbers_data_array,
        },
        coords=coords,
    )

    (
        dataset.rio.set_spatial_dims(
            x_dim="eastings",
            y_dim="northings",
            inplace=True,
        ).rio.write_crs(
            27700,
            inplace=True,
        )
    )

    data_path = shared_datadir / f"{name}.tif"

    dataset.rio.to_raster(data_path)

    return str(data_path)


@pytest.mark.parametrize(
    argnames=(
        "test_case_name",
        "expected_output_name",
    ),
    argvalues=(
        (
            "same_cell_size_same_shape",
            "same_cell_size_same_shape_dataset",
        ),
        (
            "larger_cell_size_same_shape",
            "same_cell_size_same_shape_dataset",
        ),
        (
            "smaller_cell_size_same_shape",
            "same_cell_size_same_shape_dataset",
        ),
        (
            "same_cell_size_smaller_shape",
            "same_cell_size_same_shape_dataset_with_nodata",
        ),
        (
            "larger_cell_size_smaller_shape",
            "same_cell_size_same_shape_dataset_with_nodata",
        ),
        (
            "smaller_cell_size_smaller_shape",
            "same_cell_size_same_shape_dataset_with_nodata",
        ),
    ),
    ids=(
        "Same cell size, same shape",
        "Larger cell size, same shape",
        "Smaller cell size, same shape",
        "Same cell size, smaller shape",
        "Larger cell size, smaller shape",
        "Smaller cell size, smaller shape",
    ),
)
def test_read_dataset_from_file(
    test_case_name: str,
    expected_output_name: str,
    request: FixtureRequest,
) -> None:
    """Returns the expected dataset."""
    test_case_path = request.getfixturevalue(test_case_name)
    output_dataset = _read_dataset_from_file(
        data_path=test_case_path,
        categorical=True,
        expected_cell_size=1,
        expected_x_min=0,
        expected_x_max=6,
        expected_y_max=6,
        nodata=0,
    )
    # remove attrs that are created by **reading** a dataset
    # the attrs are correct but do not exist in the dataset created directly in memory
    output_dataset.spatial_ref.attrs.pop("GeoTransform")
    output_dataset.ones.attrs.clear()
    output_dataset.numbers.attrs.clear()

    expected_output = request.getfixturevalue(expected_output_name)

    assert_identical(
        output_dataset,
        expected_output,
    )