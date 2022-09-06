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

#     def to_data_arrays(self) -> TiledDataArrayLayer:
#         data_arrays = (mask.to_data_array(name=self.name) for mask in self.masks)

#         return TiledDataArrayLayer(
#             name=self.name,
#             data_arrays=data_arrays,
#             metadata=self.metadata,
#         )

# temporary resample & reshape function
from pathlib import Path
from xarray import DataArray, Dataset, open_dataset
from rasterio.drivers import raster_driver_extensions

from sds_data_model.constants import CELL_SIZE
from sds_data_model._raster import _check_cellsize, _check_shape_and_extent, _resample_cellsize, _to_bng_extent
from sds_data_model._vector import _check_layer_projection


def read_dataset_from_file(
    data_path: str,
    categorical: bool=False, 
    nodata: float=None,
    band: int=1,
    engine: str=None,
    decode_coords="all",
    **data_kwargs,
) -> Dataset:
    suffix = Path(data_path).suffixes[0]
    
    if engine:
        pass
    elif suffix == ".zarr":
        engine = "zarr"
    elif suffix[1:] in raster_driver_extensions().keys():
        engine = "rasterio"
        decode_coords = None
    
    raster = open_dataset(
        data_path,
        engine=engine,
        decode_coords=decode_coords,
        mask_and_scale=False,
        **data_kwargs,
    )
    
    _check_layer_projection({"crs" : raster.rio.crs.to_string()})
    
    if raster.band.ndim:
        raster = raster.sel(band=band)
    
    if _check_cellsize(raster.rio.transform()):
        raster = _resample_cellsize(
            raster=raster.to_array().squeeze(),
            categorical=categorical,
        )
    
    if _check_shape_and_extent(raster):
        raster = _to_bng_extent(
            raster=raster.to_array().squeeze(),
            nodata=nodata,
        )
    
    return raster