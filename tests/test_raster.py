"""Tests for raster module."""
from pathlib import Path
from typing import Tuple

from numpy import arange, ones
import rioxarray  # noqa: F401 - Needed for `.rio` accessor methods
from pytest import fixture
from xarray import DataArray, Dataset
from xarray.testing import assert_identical

from sds_data_model.raster import read_dataset_from_file


@fixture
def same_cell_size_same_shape(
    shared_datadir: Path,
    xmin: int = 0,
    xmax: int = 6,
    ymin: int = 0,
    ymax: int = 6,
    cell_size: int = 1,
    name: str = "same_cell_size_same_shape",
) -> Tuple[str, Dataset]:
    """A dataset with the same cell size and same shape."""
    data = ones(shape=(ymax, xmax), dtype="uint8")

    coords = {
        "northings": arange(ymax - (cell_size / 2), ymin, -cell_size),
        "eastings": arange(xmin + (cell_size / 2), xmax, cell_size),
    }

    data_array = DataArray(
        data=data,
        coords=coords,
    )

    dataset = Dataset(
        data_vars={
            name: data_array,
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

    return (str(data_path), dataset)


def test_read_dataset_from_file(
    same_cell_size_same_shape: Tuple[str, Dataset],
) -> None:
    """Returns the expected dataset."""
    output_dataset = read_dataset_from_file(
        data_path=same_cell_size_same_shape[0],
        expected_cell_size=1,
        expected_x_min=0,
        expected_y_max=6,
        nodata=0.0,
    )

    assert_identical(
        output_dataset,
        same_cell_size_same_shape[1],
    )
