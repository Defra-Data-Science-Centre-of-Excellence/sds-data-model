"""Tests for `DataFrameWrapper.to_zarr`."""

# from pathlib import Path

# import pytest
import rioxarray  # noqa: F401

# from pytest import raises
from xarray import Dataset, open_dataset
from xarray.testing import assert_identical

# from sds_data_model.dataframe import DataFrameWrapper

# def test_to_zarr_with_metadata(
#     hl_zarr_path_with_metadata: str,
#     expected_hl_dataset_with_metadata: Dataset,
# ) -> None:
#     """Check that attrs in the zarr look as expected."""
#     hl_dataset = open_dataset(
#         hl_zarr_path_with_metadata,
#         engine="zarr",
#         decode_coords=True,
#         chunks={
#             "eastings": 10_000,
#             "northings": 10_000,
#         },
#     )
#     assert hl_dataset.attrs == expected_hl_dataset_with_metadata.attrs


def test_to_zarr_mask(geometry_mask_path: str, expected_geometry_mask: Dataset) -> None:
    """Check geometry mask generation."""
    geometry_mask = open_dataset(
        geometry_mask_path,
        engine="zarr",
        decode_coords=True,
        chunks={
            "eastings": 1,
            "northings": 1,
        },
    )
    assert_identical(geometry_mask, expected_geometry_mask)


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
