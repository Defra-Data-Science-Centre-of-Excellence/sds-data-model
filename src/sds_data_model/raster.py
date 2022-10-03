"""Raster wrapper class."""
# from dataclasses import dataclass
# from pathlib import Path
# from typing import Generator

# from fsspec import get_mapper
# from numpy import arange, ndarray
# from xarray import DataArray

# from sds_data_model.constants import CELL_SIZE, BoundingBox
# from sds_data_model.metadata import Metadata


# @dataclass
# class TiledDataArrayLayer:
#     name: str
#     data_arrays: Generator[DataArray, None, None]
#     metadata: Metadata

#     def to_netcdfs(self, root: str) -> None:
#         base_path = Path(root) / self.name
#         if root.startswith("s3://"):
#             s3_path = get_mapper(
#                 str(base_path),
#                 s3_additional_kwargs={"ACL": "bucket-owner-full-control"},
#             )
#         else:
#             if not base_path.exists():
#                 base_path.mkdir(parents=True)
#         for index, data_array in enumerate(self.data_arrays):
#             out_path = base_path / f"{index}.nc"
#             data_array.to_netcdf(out_path)


# @dataclass
# class MaskTile:
#     bbox: BoundingBox
#     array: ndarray

#     def to_data_array(self, name: str) -> DataArray:
#         xmin, ymin, xmax, ymax = self.bbox
#         return DataArray(
#             data=self.array,
#             coords={
#                 "northings": ("northings", arange(ymax, ymin, -CELL_SIZE)),
#                 "eastings": ("eastings", arange(xmin, xmax, CELL_SIZE)),
#             },
#             name=name,
#         )


# @dataclass
# class TiledMaskLayer:
#     name: str
#     masks: Generator[MaskTile, None, None]
#     metadata: Metadata

#    def to_data_arrays(self) -> TiledDataArrayLayer:
#        data_arrays = (mask.to_data_array(name=self.name) for mask in self.masks)

#         return TiledDataArrayLayer(
#             name=self.name,
#             data_arrays=data_arrays,
#             metadata=self.metadata,
#         )


from dataclasses import dataclass
from typing import Dict, List, Literal, Optional, Type, TypeVar, Union

from xarray import Dataset

from sds_data_model._raster import _read_dataset_from_file

_DatasetWrapper = TypeVar("_DatasetWrapper", bound="DatasetWrapper")


@dataclass
class DatasetWrapper:
    """#TODO DatasetWrapper class documentation."""

    dataset: Dataset

    @classmethod
    def from_files(
        cls: Type[_DatasetWrapper],
        data_path: str,
        bands: Union[List[int], List[str], None] = None,
        categorical: Union[bool, Dict[Union[int, str], bool]] = False,
        nodata: Optional[float] = None,
        engine: Optional[str] = None,
        decode_coords: Optional[Union[bool, Literal["coordinates", "all"]]] = "all",
    ):
        """Read in a raster from file at 10m cell size and British National Grid extent.

        Examples:
            >>> from raster import DatasetWrapper
            >>> dset_wrap_from_tif=DatasetWrapper.from_files(
                data_path="3_band_raster.tif",
                bands=[1, 2],
                categorical={1 : True, 2 : False},
                )
            >>> # read 2 bands from raster, specifying both are categorical.
            >>> dset_wrap_from_zarr=DatasetWrapper.from_files(
                data_path="multiband_raster.zarr",
                bands=["classification", "code"],
                categorical=True
                )

        Args:
            data_path (str): File path to raster.
            bands (Union[List[int], List[str], None], optional):  List of bands
                to select from the raster. Defaults to None.
            categorical (Union[bool, Dict[Union[int, str], bool]], optional):
                `bool` or `dict` mapping ({band : bool}) of the interpolation
                used to resample. Defaults to False.
            nodata (Optional[float], optional): Value that will fill the grid where
                there is no data (if it is not `None`). Defaults to None.
            engine (Optional[str], optional): _description_. Engine used by
                `open_dataset`. Defaults to None.
            decode_coords (Optional[Union[bool, Literal["coordinates", "all"]]],
             optional):
                Value used by `open_dataset`. Variable upon `engine` selection.
                Defaults to "all".

        Returns:
            DatasetWrapper
        """
        dataset = _read_dataset_from_file(
            data_path=data_path,
            bands=bands,
            categorical=categorical,
            nodata=nodata,
            engine=engine,
            decode_coords=decode_coords,
        )

        return cls(
            dataset=dataset,
        )
