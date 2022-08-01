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
from pyogrio.core import ogr_read_info
from rasterio.features import geometry_mask, rasterize
from rasterio.dtypes import get_minimum_dtype
from shapely.geometry import box
from shapely.geometry.base import BaseGeometry
from xarray import DataArray
from osgeo.ogr import Open, Layer

from sds_data_model.constants import (
    BNG,
    BNG_XMAX,
    BNG_XMIN,
    BNG_YMAX,
    BNG_YMIN,
    CELL_SIZE,
    OUT_SHAPE,
    BoundingBox,
)
from sds_data_model.metadata import Metadata

CategoryLookup = Dict[str, Dict[int, str]]
CategoryLookups = Dict[str, CategoryLookup]
Schema = Dict[str, str]


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


def _get_schema(
    data_path: str,
    **kwargs,
) -> Dict[str, str]:
    """Uses pyogrio.core.ogr_read_info to read file fields and dtypes and return as dict"""
    return {
        fields: dtypes 
        for fields, dtypes in zip(
            *map(
                ogr_read_info(data_path, **kwargs).get, ["fields", "dtypes"]
            )
        )
    }
    

def _get_categorical_column(
    df: DataFrame,
    column_name: str,
) -> Series:
    column = df.loc[:, column_name]
    return column.astype("category")

def _get_category_lookup(
    categorical_column: Series,
) -> CategoryLookup:
    return {index: category for index, category in enumerate(categorical_column.cat.categories)}

def _get_category_dtype(
    categorical_column: Series,    
) -> str:
    return str(categorical_column.cat.codes.dtype)

def _get_categories_and_dtypes(
    data_path: str,
    convert_to_categorical: List[str],
    data_kwargs: Optional[Dict[str, str]] = None,
) -> Tuple[CategoryLookups, Schema]:
            """Category and dtype looks for each column."""
    if data_kwargs:
        df = read_dataframe(
            data_path,
            read_geometry=False,
            columns=[convert_to_categorical],
            **data_kwargs,
        )
    else:
        df = read_dataframe(
            data_path,
            read_geometry=False,
            columns=[convert_to_categorical],
        )
    categorical_columns = tuple(
        (
            column_name,
            _get_categorical_column(
                df=df,
                column_name=column_name,
            ),
         ) for column_name in convert_to_categorical
    )
    category_lookups = {
        column_name: _get_category_lookup(categorical_column) for column_name, categorical_column in categorical_columns
    }
    category_dtypes = {
        column_name: _get_category_dtype(categorical_column) for column_name, categorical_column in categorical_columns
    }
    return (category_lookups, category_dtypes)

def _get_index_of_category(
    category_lookup: CategoryLookup,
    category: str,
) -> int:
    return list(category_lookup.values()).index(category)


def _get_code_for_category(
    category_lookup: CategoryLookup,
    category: str,
) -> str:
    index = _get_index_of_category(
        category_lookup=category_lookup,
        category=category,
    )
    return list(category_lookup.keys())[index]


def _recode_categorical_strings_ed(
    gpdf: GeoDataFrame,
    column: str,
    category_lookups: CategoryLookups,
) -> GeoDataFrame:
    """Returns a GeoDataFrame where an integer representation of a categorical column specified by the user is assigned
    to the GeoDataFrame - string representation is dropped but mapping is stored in self.category_lookup."""
    gpdf.loc[:, column] = gpdf.loc[:, column].apply(
        lambda category: _get_code_for_category(
            category_lookup=category_lookups[column],
            category=category,
        )
    )
    return gpdf


def _get_layer(
    path: str, **kwargs
) -> Layer:
    """Returns a single layer from an OGR-compatible vector file.

    If no layer identifier is provided then the first (0th) layer is returned.

    Args:
        path (str): Path to OGR-compatible vector file. 

    Returns:
        Layer: An ogr.Layer object.
    """
    if "layer" in kwargs:
        iLayer = kwargs.get("layer")
    else:
        iLayer = 0

    dataset = Open(path)
    layer = dataset.GetLayer(iLayer=iLayer)

    return(layer)

def _check_layer_projection(
    path: str, **kwargs
) -> None:
    """Checks whether the projection of an OGR-compatible vector layer is British National Grid (EPSG:27700).

    Args:
        path (str): Path to OGR-compatible vector file.

    Raises:
        Exception: When projection of input layer does not match the British National Grid Spatial Reference System. 
    """    
    layer = _get_layer(path, **kwargs)
    layer_prj = layer.GetSpatialRef()
    
    # check if projection of input data matches BNG stated in constants
    if(layer_prj.IsSame(BNG)):
        # add any logging requirements
        print("Logging gubbins to go here")
    else:
        raise Exception("Input dataset not in British National Grid. Reproject source data.")
