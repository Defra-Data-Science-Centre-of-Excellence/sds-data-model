from asyncio import run
from dataclasses import asdict
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

from affine import Affine
from dask import delayed
from dask.array import block, from_delayed
from dask.delayed import Delayed
from geopandas import GeoDataFrame, read_file
from more_itertools import chunked
from numpy import arange, ones, zeros
from pandas import DataFrame, Series, merge
from rasterio.features import geometry_mask, rasterize
from rasterio.dtypes import get_minimum_dtype
from shapely.geometry import box
from shapely.geometry.base import BaseGeometry
from xarray import DataArray
from sds_data_model.constants import (
    BNG_XMAX,
    BNG_XMIN,
    BNG_YMAX,
    BNG_YMIN,
    CELL_SIZE,
    OUT_SHAPE,
    BoundingBox,
)

from sds_data_model.metadata import Metadata


@delayed
def _from_file(
    data_path: str,
    bbox: BoundingBox,
    **kwargs,
) -> Delayed:
    return read_file(
        filename=data_path,
        bbox=box(*bbox),
        **kwargs,
    )


@delayed
def _select(gpdf: Delayed, columns: List[str]) -> Delayed:
    return gpdf[columns]


@delayed
def _where(gpdf: Delayed, condition: Series) -> Delayed:
    return gpdf[condition]


@delayed
def _join(
    gpdf: Delayed,
    other: DataFrame,
    how: str,
    fillna: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> Delayed:
    _gpdf = merge(
        left=gpdf,
        right=other,
        how=how,
        **kwargs,
    )
    if fillna:
        return _gpdf.fillna(fillna)
    else:
        return _gpdf


@delayed
def _get_mask(
    gpdf: GeoDataFrame,
    invert: bool,
    out_shape: Tuple[int, int],
    dtype: str,
    transform: Affine,
) -> Delayed:
    if all(gpdf.geometry.is_empty) and invert:
        return zeros(
            shape=out_shape,
            dtype=dtype,
        )
    elif all(gpdf.geometry.is_empty) and not invert:
        return ones(
            shape=out_shape,
            dtype=dtype,
        )
    else:
        return geometry_mask(
            geometries=gpdf.geometry,
            out_shape=out_shape,
            transform=transform,
            invert=invert,
        ).astype(dtype)


def _get_shapes(
    gpdf: GeoDataFrame,
    categorical_column: str,
) -> Generator[Tuple[BaseGeometry, Any], None, None]:
    return (
        (geometry, value)
        for geometry, value in zip(gpdf["geometry"], gpdf[categorical_column])
    )

def _get_col_dtype(
    gpdf: GeoDataFrame,
    categorical_column: str,
) -> str:
    return get_minimum_dtype(gpdf[categorical_column])


@delayed
def _to_categorical_raster(
    gpdf: GeoDataFrame,
    categorical_column: str,
    out_shape: Tuple[int, int],
    dtype: str,
    transform: Affine,
    **kwargs,
) -> Delayed:
    if all(gpdf.geometry.is_empty):
        return zeros(
            shape=out_shape,
            dtype=dtype,
        )
    else:
        shapes = _get_shapes(
            gpdf=gpdf,
            categorical_column=categorical_column,
        )
        return rasterize(
            shapes=shapes,
            out_shape=out_shape,
            transform=transform,
            dtype=dtype,
            **kwargs,
        )


def _from_delayed_to_data_array(
    delayed_arrays: Tuple[Delayed],
    name: str,
    metadata: Metadata,
    dtype: str,
) -> DataArray:
    dask_arrays = tuple(
        from_delayed(mask, dtype=dtype, shape=OUT_SHAPE) for mask in delayed_arrays
    )
    rows = chunked(dask_arrays, 7)
    data = block(list(rows))
    _metadata = asdict(metadata) if metadata else None
    return DataArray(
        data=data,
        coords={
            "northings": ("northings", arange(BNG_YMAX, BNG_YMIN, -CELL_SIZE)),
            "eastings": ("eastings", arange(BNG_XMIN, BNG_XMAX, CELL_SIZE)),
        },
        name=name,
        attrs=_metadata,
    )
