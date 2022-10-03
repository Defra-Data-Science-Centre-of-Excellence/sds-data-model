"""Tests for raster module."""
from pathlib import Path

import pytest
import rioxarray  # noqa: F401 - Needed for `.rio` accessor methods
from numpy import arange, array, ones
from pytest import FixtureRequest, fixture
from xarray import DataArray, Dataset
from xarray.testing import assert_identical

from sds_data_model.raster import _read_dataset_from_file


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


@pytest.mark.parametrize(
    argnames="test_case_name",
    argvalues=(
        "same_cell_size_same_shape",
        "larger_cell_size_same_shape",
    ),
    ids=(
        "Same cell size, same shape",
        "Larger cell size, same shape",
    ),
)
def test_read_dataset_from_file(
    same_cell_size_same_shape_dataset: Dataset,
    test_case_name: str,
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

    assert_identical(
        output_dataset,
        same_cell_size_same_shape_dataset,
    )
