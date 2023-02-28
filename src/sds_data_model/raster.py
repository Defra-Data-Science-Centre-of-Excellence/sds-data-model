"""Raster wrapper class."""
from dataclasses import dataclass
from logging import warning
from typing import Any, Dict, List, Literal, Optional, Type, TypeVar, Union

from xarray import Dataset

from sds_data_model._dataframe import _warn_zarr_overwrite
from sds_data_model._raster import _read_dataset_from_file

_DatasetWrapper = TypeVar("_DatasetWrapper", bound="DatasetWrapper")


@dataclass
class DatasetWrapper:
    """A wrapper for an `xarray.dataset`.

    This class forms a light wrapper around the `xarray.Dataset` class. It's key
    feature is a method to ingest many types of raster files, and resample the
    data to the data model format of British National Grid extent and 10m spatial
    resolution.

    During raster resampling, linear interpolation is used, unless categorical
    data is provided, in which case, nearest neighbour interpolation is applied.

    Attributes:
        dataset(Dataset): An xarray Dataset containing one or more DataArrays.

    Methods:
        from_files: Read in a raster from file at 10m cell size and
            British National Grid extent.

    Returns:
        _DatasetWrapper: Dataset

    """  # noqa: B950

    dataset: Dataset

    @classmethod
    def from_files(
        cls: Type[_DatasetWrapper],
        data_path: str,
        bands: Union[List[int], List[str], None] = None,
        categorical: Union[bool, Dict[Union[int, str], bool]] = False,
        nodata: Optional[float] = None,
        out_path: Optional[str] = None,
        overwrite: bool = False,
        chunks: Optional[Union[int, Dict[Any, Any], Literal["auto"]]] = "auto",
        engine: Optional[str] = None,
        decode_coords: Optional[Union[bool, Literal["coordinates", "all"]]] = "all",
    ) -> _DatasetWrapper:
        """Read in a raster from file at 10m cell size and British National Grid extent.

        Examples:
            Read 2 bands from raster, specifying that the first is categorical.

            >>> dset_wrap_from_tif = DatasetWrapper.from_files(
                    data_path="3_band_raster.tif",
                    bands=[1, 2],
                    categorical={1: True, 2: False},
                )

            Read 2 bands from raster, specifying both are categorical.

            >>> dset_wrap_from_zarr = DatasetWrapper.from_files(
                    data_path="multiband_raster.zarr",
                    bands=["classification", "code"],
                    categorical=True
                )

        Args:
            data_path (str): File path to raster.
            bands (Union[List[int], List[str], None]):  List of bands
                to select from the raster. Defaults to None.
            categorical (Union[bool, Dict[Union[int, str], bool]]):
                `bool` or `dict` mapping ({band : bool}) of the interpolation
                used to resample. Defaults to False.
            nodata (Optional[float]): Value that will fill the grid where
                there is no data (if it is not `None`). Defaults to None.
            out_path (Optional[str]): Path to write reshaped data. Defaults to None.
            overwrite (bool): Overwrite existing zarr? Defaults to False.
            chunks (Optional[Union[int, Dict[Any, Any], Literal["auto"]]]): Chunk size
                or state. Passed to `chunks` in `xarray.open_dataset`.
                Defaults to "auto".
            engine (Optional[str]): Engine used by `open_dataset`. Defaults to None.
            decode_coords (Optional[Union[bool, Literal["coordinates", "all"]]]):
                Value used by `open_dataset`. Variable upon `engine` selection.
                Defaults to "all".

        Returns:
            _DatasetWrapper: A thin wrapper around an `xarray.Dataset` containing
                `xarray.DataArray with 10m cell size and British National Grid extent.
        """
        if chunks == "auto":
            warning("Chunks set to auto. OOM errors could occur.")
        if out_path:
            _warn_zarr_overwrite(out_path, overwrite)

        dataset = _read_dataset_from_file(
            data_path=data_path,
            bands=bands,
            categorical=categorical,
            nodata=nodata,
            engine=engine,
            decode_coords=decode_coords,
            out_path=out_path,
            chunks=chunks,
        )
        return cls(
            dataset=dataset,
        )
