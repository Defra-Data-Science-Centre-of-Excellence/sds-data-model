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
    """Returns a delayed GeoDataFrame clipped to a given bounding box."""
    return read_file(
        filename=data_path,
        bbox=box(*bbox),
        **kwargs,
    )


@delayed
def _select(gpdf: Delayed, columns: List[str]) -> Delayed:
    """Returns given columns from a delayed GeoDataFrame."""
    return gpdf[columns]


@delayed
def _where(gpdf: Delayed, condition: Series) -> Delayed:
    """Returns a delayed GeoDataFrame filtered by a given condition."""
    return gpdf[condition]


@delayed
def _join(
    gpdf: Delayed,
    other: DataFrame,
    how: str,
    fillna: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> Delayed:
    """Returns a delayed GeoDataFrame joined to a given DataFrame."""
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
    """Returns a delayed boolean Numpy ndarray where 1 means the pixel overlaps a geometry."""
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
    column: str,
) -> Generator[Tuple[BaseGeometry, Any], None, None]:
    """Yields (Geometry, value) tuples for every row in a GeoDataFrame."""
    return (
        (geometry, value)
        for geometry, value in zip(gpdf["geometry"], gpdf[column])
    )

@delayed
def _to_raster(
    gpdf: GeoDataFrame,
    column: str,
    out_shape: Tuple[int, int],
    dtype: str,
    transform: Affine,
    **kwargs,
) -> Delayed:
    """Returns a delayed boolean Numpy ndarray with values taken from a given column."""
    if all(gpdf.geometry.is_empty):
        return zeros(
            shape=out_shape,
            dtype=dtype,
        )
    else:
        shapes = _get_shapes(
            gpdf=gpdf,
            column=column,
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
    """Converts a 1D delayed Numpy array into a 2D DataArray."""
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

def _get_categories(
        data_path: str,
        convert_to_categorical: List[str],
        data_kwargs: Dict[str, Any],
    ) -> dict:
        """Returns a nested dictionary of {column : {Integer:String}}, one per column specified as categorical by the user."""
        gpdf_non_spatial = read_file(data_path, ignore_geometry=True, **data_kwargs)

        cat_col_lookup = [dict(enumerate(gpdf_non_spatial[col].astype("category").cat.categories)) for col in convert_to_categorical]

        cat_lookup={col:cat_col_lookup[convert_to_categorical.index(col)] for col in convert_to_categorical}

        return cat_lookup

def _recode_categorical_strings(
        gpdf: GeoDataFrame,
        column: str,
        lookup: Dict[str, Any]
    ) -> GeoDataFrame:
        """Returns a GeoDataFrame where an integer representation of a categorical column specified by the user is assigned 
        to the GeoDataFrame - string representation is dropped but mapping is stored in self.category_lookup."""    
        gpdf[f'{column}_code']=gpdf[column].apply(lambda x: list(lookup[column].keys())[list(lookup[column].values()).index(x)])
            
        gpdf = gpdf.drop(columns=[f'{column}']).rename(columns={f'{column}_code' : f'{column}'})

        return gpdf