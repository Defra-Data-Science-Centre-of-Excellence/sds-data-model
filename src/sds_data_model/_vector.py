from dataclasses import asdict
from json import load
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple

from affine import Affine
from dask import delayed
from dask.array import block, from_delayed
from dask.delayed import Delayed
from geopandas import GeoDataFrame, read_file
from more_itertools import chunked
from numpy import arange, ones, zeros
from pandas import DataFrame, Series, merge
from pyogrio import read_dataframe, read_info
from rasterio.features import geometry_mask, rasterize
from rasterio.dtypes import get_minimum_dtype
from shapely.geometry import box
from shapely.geometry.base import BaseGeometry
from xarray import DataArray

from sds_data_model.constants import (
    BNG,
    BNG_XMAX,
    BNG_XMIN,
    BNG_YMAX,
    BNG_YMIN,
    CELL_SIZE,
    OUT_SHAPE,
    BoundingBox,
    CategoryLookup,
    CategoryLookups,
    Schema,
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
        (geometry, value) for geometry, value in zip(gpdf["geometry"], gpdf[column])
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


def _get_info(
    data_path: str,
    data_kwargs: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Get information about a vector file using `pyogrio.read_info`_.

    This is a thin wrapper around `pyogrio.read_info`_ that will unpack
    `data_kwargs` if it exists.

    Examples:
        >>> from pathlib import Path
        >>> from pprint import pprint
        >>> info = _get_info(
            data_path=f"/vsizip/{Path.cwd()}/tests/data/Countries__December_2021__GB_BUC.zip",
        )
        >>> pprint(info)
        {'capabilities': {'fast_set_next_by_index': 1,
                        'fast_spatial_filter': 0,
                        'random_read': 1},
        'crs': 'EPSG:27700',
        'dtypes': array(['int32', 'object', 'object', 'object', 'int32', 'int32', 'float64',
            'float64', 'object', 'float64', 'float64'], dtype=object),
        'encoding': 'UTF-8',
        'features': 3,
        'fields': array(['OBJECTID', 'CTRY21CD', 'CTRY21NM', 'CTRY21NMW', 'BNG_E', 'BNG_N',
            'LONG', 'LAT', 'GlobalID', 'SHAPE_Leng', 'SHAPE_Area'],
            dtype=object),
        'geometry_type': 'Polygon'}

    Args:
        data_path (str): Path to the vector file.
        data_kwargs (Optional[Dict[str, Any]], optional): A dictionary of key word arguments to be
            passed to`pyogrio.read_info`_. Defaults to None.

    Returns:
        Dict[str, Any]: A dictionary of information about the vector file.

    .. _pyogrio.read_info:
        https://pyogrio.readthedocs.io/en/latest/api.html#pyogrio.read_info

    """
    if data_kwargs:
        return read_info(
            data_path,
            **data_kwargs,
        )
    else:
        return read_info(
            data_path,
        )


def _get_schema(
    info: Dict[str, Any],
) -> Schema:
    """Generate schema from info returned by :func:`_get_info`.

    Examples:
        >>> from pathlib import Path
        >>> from pprint import pprint
        >>> info = _get_info(
            data_path=f"/vsizip/{Path.cwd()}/tests/data/Countries__December_2021__GB_BUC.zip",
        )
        >>> schema = _get_schema(info)
        >>> pprint(schema)
        {'BNG_E': 'int32',
        'BNG_N': 'int32',
        'CTRY21CD': 'object',
        'CTRY21NM': 'object',
        'CTRY21NMW': 'object',
        'GlobalID': 'object',
        'LAT': 'float64',
        'LONG': 'float64',
        'OBJECTID': 'int32',
        'SHAPE_Area': 'float64',
        'SHAPE_Leng': 'float64'}

    Args:
        info (Dict[str, Any]): The dictionary of information about the vector file
            returned by :func:`_get_info`.

    Returns:
        Schema: A dictionary that maps column names to data types.
    """
    zipped_fields_and_dtypes = zip(*tuple(info[key] for key in ("fields", "dtypes")))
    return {field: dtype for field, dtype in zipped_fields_and_dtypes}


def _get_gpdf(
    data_path: str,
    data_kwargs: Optional[Dict[str, Any]] = None,
) -> GeoDataFrame:
    """Read a vector file into a `geopandas.GeoDataFrame`_ using `pyogrio.read_dataframe`_.

    This is a thin wrapper around `pyogrio.read_dataframe`_ that will unpack
    `data_kwargs` if it exists.

    Examples:
        >>> from pathlib import Path
        >>> _get_gpdf(
            data_path=f"/vsizip/{Path.cwd()}/tests/data/Countries__December_2021__GB_BUC.zip",
        )
        OBJECTID   CTRY21CD  CTRY21NM CTRY21NMW   BNG_E   BNG_N     LONG        LAT                                GlobalID    SHAPE_Leng    SHAPE_Area                                           geometry
        0         1  E92000001   England    Lloegr  394883  370883 -2.07811  53.235001  {40A19681-7FE5-401C-A464-E58C7667E4D9}  4.616392e+06  1.306811e+11  MULTIPOLYGON (((87767.569 8868.285, 89245.065 ...
        1         2  S92000003  Scotland  Yr Alban  277744  700060 -3.97094  56.177399  {605FF196-6A26-498B-B944-DF9E6C2A726D}  9.816915e+06  7.865483e+10  MULTIPOLYGON (((202636.001 599689.300, 201891....
        2         3  W92000004     Wales     Cymru  263405  242881 -3.99417  52.067402  {D31B6005-7F16-4EF2-9BD1-B8CD8CCA5E66}  1.555750e+06  2.081868e+10  MULTIPOLYGON (((215005.297 196592.421, 214331....

    Args:
        data_path (str): Path to the vector file.
        data_kwargs (Optional[Dict[str, Any]], optional): A dictionary of key word arguments to be
            passed to`pyogrio.read_dataframe`_. Defaults to None.

    Returns:
        GeoDataFrame: The `geopandas.GeoDataFrame`_ returned by `pyogrio.read_dataframe`_.

    .. _geopandas.GeoDataFrame:
        https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html

    .. _pyogrio.read_dataframe:
        https://pyogrio.readthedocs.io/en/latest/api.html#pyogrio.read_dataframe

    """
    if data_kwargs:
        return read_dataframe(
            data_path,
            **data_kwargs,
        )
    else:
        return read_dataframe(
            data_path,
        )


def _get_categorical_column(
    df: DataFrame,
    column_name: str,
) -> Series:
    column = df.loc[:, column_name]
    return column.astype("category")


def _get_category_lookup(
    categorical_column: Series,
) -> CategoryLookup:
    return {
        index + 1: category
        for index, category in enumerate(categorical_column.cat.categories)
    }


def _get_category_dtype(
    categorical_column: Series,
) -> str:
    dtype = categorical_column.cat.codes.dtype
    if dtype == "int8":
        return "uint8"
    else:
        return str(dtype)

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
        _data_kwargs = {
            "read_geometry": False,
            "columns": [convert_to_categorical],
            **data_kwargs,
        }
    else:
        _data_kwargs = {
            "read_geometry": False,
            "columns": [convert_to_categorical],
        }

    df = _get_gpdf(
        data_path=data_path,
        data_kwargs=_data_kwargs,
    )
    categorical_columns = tuple(
        (
            column_name,
            _get_categorical_column(
                df=df,
                column_name=column_name,
            ),
        )
        for column_name in convert_to_categorical
    )
    category_lookups = {
        column_name: _get_category_lookup(categorical_column)
        for column_name, categorical_column in categorical_columns
    }
    category_dtypes = {
        column_name: _get_category_dtype(categorical_column)
        for column_name, categorical_column in categorical_columns
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


def _recode_categorical_strings(
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


def _check_layer_projection(
    info: Dict[str, Any],
) -> None:
    """Checks whether the projection is British National Grid (EPSG:27700)

    Args:
        info (Dict[str, Any]): The dictionary of information returned by `_get_info`.

    Raises:
        TypeError: When projection is not British National Grid.
    """
    crs = info["crs"]
    if crs != "EPSG:27700":
        raise TypeError(
            f"Projection is {crs}, not British National Grid (EPSG:27700). Reproject source data."
        )


def _get_metadata(data_path: str, metadata_path: str) -> Optional[Metadata]:
    """Read metadata from path, or json sidecar, or return None."""
    json_sidecar = Path(f"{data_path}-metadata.json")
    if not metadata_path and json_sidecar.exists():
        with open(json_sidecar, "r") as json_metadata:
            metadata = load(json_metadata)
    elif not metadata_path:
        metadata = None
    else:
        metadata = Metadata.from_file(metadata_path)
    return metadata
