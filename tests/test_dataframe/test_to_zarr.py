"""Tests for `DataFrameWrapper.to_zarr`."""

# from pathlib import Path
from typing import Callable, List, Optional

import pytest

# import pytest
import rioxarray  # noqa: F401
from pytest import FixtureRequest
from xarray import open_dataset
from xarray.testing import assert_identical


@pytest.mark.parametrize(
    argnames=(
        "name",
        "columns",
        "expected_dataset_name",
    ),
    argvalues=(
        ("mask", None, "expected_mask_dataset"),
        ("int16", ["int16"], "expected_int16_dataset"),
        ("int32", ["int32"], "expected_int32_dataset"),
        ("int64", ["int64"], "expected_int64_dataset"),
        ("float32", ["float32"], "expected_float32_dataset"),
        ("float64", ["float64"], "expected_float64_dataset"),
    ),
    ids=(
        "mask",
        "int16",
        "int32",
        "int64",
        "float32",
        "float64",
    ),
)
def test_to_zarr_mask(
    name: str,
    columns: Optional[List[str]],
    expected_dataset_name: str,
    request: FixtureRequest,
    make_expected_dataset_path: Callable[[str, Optional[List[str]]], str],
) -> None:
    """Check geometry mask generation."""
    expected_dataset_path = make_expected_dataset_path(name, columns)
    dataset = open_dataset(
        expected_dataset_path,
        engine="zarr",
        decode_coords=True,
        chunks={
            "eastings": 1,
            "northings": 1,
        },
    )
    expected_dataset = request.getfixturevalue(expected_dataset_name)
    assert_identical(dataset, expected_dataset)


# @pytest.mark.parametrize(
#     argnames=[
#         "out_path",
#     ],
#     argvalues=[
#         ("",),
#         ("hl.zarr",),
#     ],
#     ids=[
#         "directory",
#         ".zarr file",
#     ],
# )
# def test_zarr_overwrite_check(
#     out_path: str,
#     tmp_path: Path,
#     hl_wrapper_no_metadata: DataFrameWrapper,
# ) -> None:
#     """Check error thrown when a zarr output path already contains a zarr."""
#     with raises(ValueError, match="Zarr file already exists"):

#         hl_wrapper_no_metadata.to_zarr(
#             path=str(tmp_path / out_path),
#         )

#         hl_wrapper_no_metadata.to_zarr(
#             path=str(tmp_path / out_path),
#         )
