"""Tests for raster module."""

import pytest
import rioxarray  # noqa: F401 - Needed for `.rio` accessor methods
from pytest import FixtureRequest
from xarray.testing import assert_identical

from sds_data_model._raster import _read_dataset_from_file


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
        out_path="__test_out.zarr__",
        expected_cell_size=1,
        expected_x_min=0,
        expected_x_max=6,
        expected_y_max=6,
        nodata=0,
    ).load()
    # remove attrs that are created by **reading** a dataset
    # the attrs are correct but do not exist in the dataset created directly in memory
    output_dataset.spatial_ref.attrs.pop("GeoTransform")
    output_dataset.ones.attrs.clear()
    output_dataset.numbers.attrs.clear()
    for var in output_dataset.variables:
        if "_FillValue" in output_dataset[var].attrs:
            del output_dataset[var].attrs["_FillValue"]

    expected_output = request.getfixturevalue(expected_output_name)

    assert_identical(
        output_dataset,
        expected_output,
    )
