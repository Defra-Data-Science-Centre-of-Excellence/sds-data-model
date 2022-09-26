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


# temporary func to read raster + resample & reshape if necessary
from pathlib import Path
from typing import List, Literal, Optional, Sequence, Union

from rasterio.drivers import raster_driver_extensions
from xarray import Dataset, open_dataset

from sds_data_model.constants import BNG_XMIN, BNG_YMAX, CELL_SIZE
from sds_data_model._raster import _resample_and_reshape
from sds_data_model._vector import _check_layer_projection


def read_dataset_from_file(
    data_path: str,
    bands: Union[List[int], List[str], None] = None,
    categorical: Union[bool, Sequence[bool]] = False,
    nodata: Optional[float] = None,
    engine: Optional[str] = None,
    decode_coords: Union[bool, None, Literal["coordinates", "all"]] = "all",
    expected_cell_size: int = CELL_SIZE,
    expected_x_min: int = BNG_XMIN,
    expected_y_max: int = BNG_YMAX,
) -> Dataset:
    """# TODO.

    Args:
        data_path (str): #TODO
        band (Union[List[int], List[str], None], optional): #TODO. Defaults to None.
        categorical (Union[bool, Sequence[bool]], optional): #TODO. Defaults to False.
        nodata (Optional[float], optional): #TODO. Defaults to None.
        engine (Optional[str], optional): #TODO. Defaults to None.
        decode_coords (Union[bool, None, Literal["coordinates", "all"]], optional):
         #TODO. Defaults to "all".

    Returns:
        Dataset: #TODO
    """
    suffix = Path(data_path).suffixes[0]

    if engine:
        _engine = engine
    elif suffix == ".zarr":
        _engine = "zarr"
        _decode_coords = decode_coords
    elif suffix[1:] in raster_driver_extensions().keys():
        _engine = "rasterio"
        _decode_coords = None

    dataset = open_dataset(
        data_path,
        engine=_engine,
        decode_coords=_decode_coords,
        mask_and_scale=False,
    )

    _check_layer_projection({"crs": dataset.rio.crs.to_string()})

    return _resample_and_reshape(
        dataset=dataset,
        bands=bands,
        categorical=categorical,
        nodata=nodata,
        expected_cell_size=expected_cell_size,
        expected_x_min=expected_x_min,
        expected_y_max=expected_y_max,
    )
