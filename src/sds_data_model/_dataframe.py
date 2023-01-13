"""Private functions for the DataFrame wrapper class."""
from dataclasses import asdict
from itertools import chain
from json import load
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, List, Optional, Tuple, Union, cast

from affine import Affine
from bng_indexer import wkt_from_bng
from geopandas import GeoDataFrame, GeoSeries
from numpy import (
    arange,
    array,
    can_cast,
    full,
    generic,
    min_scalar_type,
    nan,
    nanmax,
    nanmin,
)
from numpy.typing import NDArray
from pandas import DataFrame as PandasDataFrame
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.column import Column
from pyspark.sql.functions import col, create_map, lit
from pyspark.sql.functions import max as _max
from pyspark.sql.functions import min as _min
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, FloatType, Row
from rasterio.env import GDALVersion
from rasterio.features import rasterize
from rioxarray.rioxarray import affine_to_coords
from shapely.geometry.base import BaseGeometry
from shapely.wkt import loads
from xarray import DataArray, Dataset, merge, open_dataset

from sds_data_model.metadata import Metadata
from sds_data_model.dataframe import DataFrameWrapper
from graphviz import Digraph


@udf(returnType=ArrayType(FloatType()))
def _bng_to_bounds(grid_reference: str) -> Tuple[float, float, float, float]:
    """_summary_.

    Args:
        grid_reference (str): _description_

    Returns:
        Tuple[float, float, float, float]: _description_
    """
    wkt = wkt_from_bng(grid_reference)
    bounds: Tuple[float, float, float, float] = loads(wkt).bounds
    return bounds


def _get_name(
    metadata: Optional[Metadata] = None,
    name: Optional[str] = None,
) -> str:
    """Gets name provided.

    Returns the provided name, the associated metadata title,
     or raises an error.

    Examples:
        If `name` is provided, the function returns that name:
        >>> _get_name(
            name="ramsar",
        )
        'ramsar'

        If `name` isn't provided but a :class: Metadata object is,
        the function returns `metadata.title`:
        >>> metadata = _get_metadata(
            data_path="tests/test_metadata/ramsar.gpkg",
            metadata_path="tests/test_metadata/ramsar.xml",
        )
        >>> _get_name(
            metadata=metadata,
        )
        'Ramsar (England)'

        If both are provided, `name` is preferred:
        >>> metadata = _get_metadata(
            data_path="tests/test_metadata/ramsar.gpkg",
            metadata_path="tests/test_metadata/ramsar.xml",
        )
        >>> _get_name(
            name="ramsar",
            metadata=metadata,
        )
        'ramsar'

        If neither are provided, an error is raised:
        >>> _get_name()
        ValueError: If there isn't any metadata, a name must be supplied.

    Args:
        metadata (Optional[Metadata]): A :class: Metadata object containing
            information parsed from GEMINI XML. Defaults to None.
        name (Optional[str]): A name, provided by the caller. Defaults to None.

    Raises:
        ValueError: If neither a name nor a `Metadata` are provided.

    Returns:
        str: A name for the dataset.
    """
    if name:
        return name
    elif metadata and metadata.title:
        return metadata.title
    else:
        raise ValueError("If there isn't any metadata, a name must be supplied.")


def _get_metadata(
    data_path: str,
    metadata_path: Optional[str] = None,
    metadata_kwargs: Optional[Dict[str, Any]] = None,
) -> Optional[Metadata]:
    """Read metadata from path, or json sidecar, or return None.

    Examples:
        If `metadata_path` is provided, the function will read that:
        >>> metadata = _get_metadata(
            data_path="tests/test_metadata/ramsar.gpkg",
            metadata_path="tests/test_metadata/ramsar.xml",
        )
        >>> metadata.title
        'Ramsar (England)'

        If `metadata_path` isn't provided but a json `sidecar`_ file exists,
        the function will read that:
        >>> from os import listdir
        >>> listdir("tests/test_metadata")
        ['ramsar.gpkg', 'ramsar.gpkg-metadata.json']

        >>> metadata = _get_metadata(
            data_path="tests/test_metadata/ramsar.gpkg",
        )
        >>> metadata.title
        'Ramsar (England)'

        If `metadata_path` isn't provided and there isn't a json sidecar file,
        the function will return `None`:
        >>> from os import listdir
        >>> listdir("tests/test_metadata")
        ['ramsar.gpkg']

        >>> metadata = _get_metadata(
            data_path="tests/test_metadata/ramsar.gpkg",
        )
        >>> metadata is None
        True

    Args:
        data_path (str): Path to the vector file.
        metadata_path (Optional[str], optional): Path to a `UK GEMINI`_ metadata file.
            Defaults to None.
        metadata_kwargs (Optional[Dict[str, Any]], optional): Key word arguments to
            be passed to the requests `get`_ method when reading xml metadata from
            a URL. Defaults to None.

    Returns:
        Optional[Metadata]: An instance of :class: Metadata
        .. _`UK GEMINI`:
            https://www.agi.org.uk/uk-gemini/
        .. _`sidecar`:
            https://en.wikipedia.org/wiki/Sidecar_file
    """
    json_sidecar = Path(f"{data_path}-metadata.json")
    if metadata_path:
        metadata = Metadata.from_file(metadata_path, metadata_kwargs)
    elif not metadata_path and json_sidecar.exists():
        with open(json_sidecar, "r") as json_metadata:
            metadata_dictionary = load(json_metadata)
            metadata = Metadata(**metadata_dictionary)
    else:
        metadata = None
    return metadata


def _check_sparkdataframe(data: Any) -> SparkDataFrame:  # noqa ANN401
    """Check input object is a `pyspark.sql.DataFrame`.

    Args:
        data (Any): Input object.

    Raises:
        ValueError: If object is not of type `pyspark.sql.DataFrame`

    Returns:
        SparkDataFrame: `SparkDataFrame`
    """
    if not isinstance(data, SparkDataFrame):
        data_type = type(data)
        raise ValueError(
            f"`self.data` must be a `pyspark.sql.DataFrame` not a {data_type}"
        )
    else:
        return data


dtype_nodata_value = {
    # "bool": False,
    # "int8": 127,
    "uint8": 255,
    "int16": 32767,
    "uint16": 65535,
    "int32": 2147483647,
    "uint32": 4294967295,
    "int64": 9223372036854775807,
    "uint64": 18446744073709551615,
    # "float16": nan,
    "float32": nan,
    "float64": nan,
}

_GDAL_AT_LEAST_35, _GDAL_AT_LEAST_37 = map(
    GDALVersion.runtime().at_least, ("3.5", "3.7")
)


def _get_minimum_dtype(array: NDArray[generic]) -> str:  # noqa: C901
    """Return minimum type that has rasterization support.

    Args:
        array (NDArray[generic]): Input data.

    Raises:
        ValueError: If not gdal version >= 3.5 and minimum type is int64.
        ValueError: If not gdal version >= 3.5 and minimum type is uint64.

    Returns:
        str: Minimum type.
    """
    array_min, array_max = nanmin(array), nanmax(array)
    minmax_types = min_scalar_type(array_min), min_scalar_type(array_max)
    if "float64" in minmax_types:
        return "float64"
    elif "float32" in minmax_types:
        return "float32"
    #     elif "float16" in minmax_types:
    #         return "float16"
    elif "int64" in minmax_types and not _GDAL_AT_LEAST_35:
        raise ValueError("int64 is out of range for supported dtypes")
    elif "int64" in minmax_types:
        return "int64"
    elif "int32" in minmax_types:
        return "int32"
    elif "int16" in minmax_types:
        return "int16"
    #     elif "int8" in minmax_types and _GDAL_AT_LEAST_37:
    #         return "int8"
    elif "uint64" in minmax_types and not _GDAL_AT_LEAST_35:
        raise ValueError("uint64 is out of range for supported dtypes")
    elif "uint64" in minmax_types:
        return "uint64"
    elif "uint32" in minmax_types:
        return "uint32"
    elif "uint16" in minmax_types:
        return "uint16"
    # elif "uint8" in minmax_types:
    else:
        return "uint8"


#     else:
#         return "bool"


def _map_dictionary_on_column(
    sdf: SparkDataFrame,
    column: str,
    lookup: Dict[Any, float],
) -> SparkDataFrame:
    """Map a lookup on a column of a `SparkDataFrame`.

    Args:
        sdf (SparkDataFrame): Input DataFrame.
        column (str): Column to map lookup on.
        lookup (Dict[Any, float]): Input dictionary of `{original_value: new_value}`.

    Returns:
        SparkDataFrame: SparkDataFrame with updated column.
    """
    _map = create_map(cast(Column, [lit(x) for x in chain(*lookup.items())]))
    return sdf.withColumn(column, _map[col(column)])


def _recode_column(
    sdf: SparkDataFrame,
    column: str,
    lookup: Union[Dict[str, Dict[Any, float]], Dict],
) -> Tuple[SparkDataFrame, Dict[Any, float]]:
    """Apply an auto-generated or input lookup to a columnn of a `SparkDataFrame`.

    Args:
        sdf (SparkDataFrame): Input DataFrame.
        column (str): Column to optionally generate lookup for and map lookup on.
        lookup (Union[Dict[str, Dict[Any, float]], Dict]): Optional input `dict` of
            `{column: {original_value: new_value}}`.

    Returns:
        Tuple[SparkDataFrame, Dict[Any, float]]: Updated SparkDataFrame, lookup
            applied to the DataFrame.
    """
    if column in lookup:
        _lookup = lookup[column]
    else:
        unique = sdf.select(column).distinct()
        _lookup = dict(
            zip(unique.rdd.flatMap(lambda x: x).collect(), range(unique.count()))
        )
    return _map_dictionary_on_column(sdf, column, _lookup), _lookup


def _get_minimum_column_dtype(
    sdf: SparkDataFrame,
    column: str,
) -> str:
    """Return minimum type of a SparkDataFrame column that has rasterization support.

    Args:
        sdf (SparkDataFrame): `SparkDataFrame`.
        column (str): column to determine minmum type for.

    Returns:
        str: Minimum column type.
    """
    return _get_minimum_dtype(
        array(sdf.select(column).distinct().dropna().rdd.flatMap(lambda x: x).collect())
    )


def _next_dtype(_type: str) -> Tuple[str, float]:
    """Return the next type and nodata value from `dtype_nodata_value`.

    Args:
        _type (str): Current type.

    Returns:
        Tuple[str, float]: type, nodata
    """
    return list(dtype_nodata_value.items())[
        list(dtype_nodata_value.keys()).index(_type) + 1
    ]


def _get_minimum_dtype_and_nodata_value(
    sdf: SparkDataFrame,
    column: str,
    nodata: Union[Dict[str, float], Dict],
) -> Tuple[str, float]:
    """Determine the minimum type of a column from or influenced by nodata.

    If nodata is provided for the input column, the minimum type that can
    accomodate the data and the nodata value is selected.
    If there is no nodata value the maximum of the range of the minimum type
    is selected, unless this value is in the data.

    Args:
        sdf (SparkDataFrame): `SparkDataFrame`
        column (str): Column to determine type and nodata for.
        nodata (Union[Dict[str, float], Dict]): `dict` that can contain a
            {column: nodata} pair.

    Returns:
        Tuple[str, float]: type, nodata
    """
    _nodata: float
    dtype = _get_minimum_column_dtype(sdf, column)

    if column in nodata:
        _nodata = nodata[column]
        while not can_cast(_nodata, dtype):
            dtype, _ = _next_dtype(dtype)
    else:
        _max_value: float = cast(Row, sdf.dropna().select(_max(sdf[column])).first())[0]
        if _max_value == dtype_nodata_value[dtype]:
            dtype, _nodata = _next_dtype(dtype)
            _min_value: float = cast(Row, sdf.select(_min(sdf[column])).first())[0]
            while not can_cast(_min_value, dtype):
                dtype, _nodata = _next_dtype(dtype)
        else:
            _nodata = dtype_nodata_value[dtype]
    return dtype, _nodata


def _get_minimum_dtypes_and_nodata(
    sdf: SparkDataFrame,
    columns: Optional[List],
    nodata: Optional[Dict[str, float]],
    lookup: Optional[Dict[str, Dict[Any, float]]],
    mask_name: Optional[str],
) -> Tuple[Dict[str, str], Dict[str, float], Optional[Dict[str, Dict[Any, float]]]]:
    """Determine the minimum type of columns from or influenced by nodata.

    If no columns are provided the type and nodata will be set to "uint8" and 0
    to generate a geometry mask.
    If columns are provided `_get_minimum_dtype_and_nodata_value` is called.
    If columns are categorical and have an entry in `lookup`, nodata is added to it.

    Args:
        sdf (SparkDataFrame): `SparkDataFrame`
        columns (Optional[List]): Columns to determine type and nodata for.
        nodata (Optional[Dict[str, float]]): Optional `dict` that can contain
            {column: nodata} pairs.
        lookup (Optional[Dict[str, Dict[Any, float]]]): Optional lookup for columns.
        mask_name (Optional[str]): Name for the geometry mask.

    Raises:
        ValueError: If no columns provided as well as no `name`.

    Returns:
        Tuple[Dict[str, str], Dict[str, float], Optional[Dict[str, Dict[Any, float]]]]:
    """
    _dtype: Dict[str, str] = {}
    _nodata: Dict[str, float] = {}
    if not columns:
        if not mask_name:
            raise ValueError("`name` arg or attribute is not set.")
        _dtype[mask_name] = "uint8"
        _nodata[mask_name] = 0
    else:
        for column in columns:
            if not nodata:
                nodata = {}
            _dtype[column], _nodata[column] = _get_minimum_dtype_and_nodata_value(
                sdf, column, nodata
            )

        if lookup:
            for column in columns:
                if column in lookup:
                    if "nodata" not in lookup[column]:
                        lookup[column]["nodata"] = _nodata[column]
    return _dtype, _nodata, lookup


def _create_empty_dataset(
    column: str,
    nodata: float,
    lookup: Optional[Dict[str, Dict[Any, float]]],
    dtype: str,
    dims: Tuple[str, str],
    height: int,
    width: int,
) -> Dataset:
    """Initialize a Dataset with the size of the desired output using a nodata fill.

    Args:
        column (str): DataFrame column.
        nodata (float): nodata/fill value of the `Dataset`.
        lookup (Optional[Dict[str, Dict[Any, float]]]): lookup for the DataFrame
            to assign the lookup for a column if it exists.
        dtype (str): Data type for the `Dataset`.
        dims (Tuple[str, str]): Names of the dimensions of the `Dataset`.
        height (int): Height of the array data.
        width (int): Width of the array data.

    Returns:
        Dataset: Dataset of `height` `width`, with `name`, `nodata`
            and optionally `lookup` included.
    """
    attrs: Dict[str, Any] = {"nodata": nodata}
    if lookup:
        if column in lookup:
            attrs["lookup"] = str(lookup[column])
    return DataArray(
        data=full(shape=(height, width), fill_value=nodata, dtype=dtype),
        dims=dims,
        name=column,
        attrs=attrs,
    ).to_dataset()


def _create_dummy_dataset(
    path: str,
    dtype: Dict[str, str],
    nodata: Dict[str, float],
    mask_name: str,
    cell_size: int,
    bng_xmin: int,
    bng_xmax: int,
    bng_ymax: int,
    columns: Optional[List],
    lookup: Optional[Dict[str, Dict[Any, float]]],
    metadata: Optional[Metadata],
) -> None:
    """A dummy Dataset. It's metadata is used to create the initial `zarr` store.

    See `Appending to existing Zarr stores`_ for more details.

    Examples:
        >>> d_dataset = _create_dummy_dataset(
            path = "/path/to/dummy.zarr"
            columns=["col"],
            dtype={"col": uint8"},
            nodata={"col": 255}
        )

        >>> d_dataset
        Delayed('_finalize_store-31bc6052-52db-49e8-bc87-fc8f7c6801ed')

    Args:
        path (str): Path to save the zarr file including file name.
        columns (Optional[List]): Columns to include in the zarr.
        dtype (Dict[str, str]): Data type for the array of each column.
        nodata (Dict[str, str]): nodata value for the array of each column.
        lookup (Optional[Dict[str, Dict[Any, Float]]]): lookup for a column if applicable.
        mask_name (str): Name for the geometry mask.
        metadata (Optional[str]): Metadata object relating to data.
        cell_size (int): The resolution of the cells in the DataArray.
        bng_xmin (int): The minimum x value of the DataArray.
        bng_xmax (int): The maximum x value of the DataArray.
        bng_ymax (int): The maximum y value of the DataArray.

    .. _`Appending to existing Zarr stores`:
        https://docs.xarray.dev/en/stable/user-guide/io.html#appending-to-existing-zarr-stores  # noqa: B950
    """
    dims = ("northings", "eastings")
    height = int(bng_ymax / cell_size)
    width = int(bng_xmax / cell_size)
    transform = Affine(cell_size, 0, bng_xmin, 0, -cell_size, bng_ymax)

    if not columns:
        columns = [mask_name]
    dataset = merge(
        _create_empty_dataset(
            column,
            nodata[column],
            lookup,
            dtype[column],
            dims,
            height,
            width,
        )
        for column in columns
    )
    dataset.update(
        affine_to_coords(
            transform,
            height=height,
            width=width,
            y_dim=dims[0],
            x_dim=dims[1],
        ),
    )
    dataset.rio.write_crs("EPSG:27700", inplace=True)
    dataset.rio.write_transform(transform, inplace=True)
    dataset.attrs = asdict(metadata) if metadata else {}
    dataset.to_zarr(
        store=path,
        mode="w",
        compute=False,
    )


def _to_zarr_region(
    pdf: PandasDataFrame,
    path: str,
    columns: List[str],
    dtype: Dict[str, str],
    nodata: Dict[str, float],
    mask_name: str,
    cell_size: int,
    out_shape: Tuple[int, int],
    bng_ymax: int,
    geometry_column_name: str,
) -> PandasDataFrame:
    """Rasterises a BNG grid cell and writes to the relevant region of a `zarr` file.

    This function will be converted into a `pandas_udf`_ and called by `applyInPandas`_
    within the :meth:`DataFrameWrapper.to_zarr` method.

    Writing to a zarr file is a side effect of the method hence the input and the
    output is the same Pandas DataFrame.

    This function assumes that the Pandas DataFrame contains a column with BNG bounds
    that is named "bounds".

    Args:
        pdf (PandasDataFrame): A Pandas DataFrame.
        path (str): Path to save the zarr file including file name.
        columns (List[str]): Columns to include in the zarr.
        dtype (Dict[str, str]): Data type for the array of each column.
        nodata (Dict[str, float]): nodata value for the array of each column.
        mask_name (str): Name for the geometry mask.
        cell_size (int): The resolution of the cells in the DataArray.
        out_shape (Tuple[int, int]): The shape (height, width) of the DataArray.
        bng_ymax (int): The maximum y value of the DataArray.
        geometry_column_name (str): The name of the geometry column in the
            Pandas DataFrame.

    Returns:
        PandasDataFrame: The input Pandas DataFrame is returned unchanged.

    .. _`pandas_udf`:
        https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.functions.pandas_udf.html  # noqa: B950

    .. _`applyInPandas`:
        https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.GroupedData.applyInPandas.htm
    """
    minx, miny, maxx, maxy = pdf["bounds"][0]
    transform = Affine(cell_size, 0, minx, 0, -cell_size, maxy)
    gpdf = GeoDataFrame(
        data=pdf,
        geometry=GeoSeries.from_wkb(pdf[geometry_column_name]),
        crs="EPSG:27700",
    )
    shapes: Union[
        List[BaseGeometry], Generator[Iterable[Tuple[BaseGeometry, Any]], None, None]
    ]
    if not columns:
        columns = [mask_name]
        shapes = [gpdf.geometry]
    else:
        shapes = (zip(gpdf.geometry, gpdf[column]) for column in columns)

    dims = ("northings", "eastings")
    dataset = merge(
        DataArray(
            data=rasterize(
                shapes=_shapes,
                out_shape=out_shape,
                fill=nodata[column],
                transform=transform,
                dtype=dtype[column],
            ),
            dims=dims,
            name=column,
        ).to_dataset()
        for column, _shapes in zip(columns, shapes)
    )

    dataset.update(
        {
            "northings": arange(maxy - (cell_size / 2), miny, -cell_size),
            "eastings": arange(minx + (cell_size / 2), maxx, cell_size),
        }
    )
    dataset.to_zarr(
        store=path,
        mode="r+",
        region={
            dims[0]: slice(
                int((maxy - bng_ymax) / -cell_size),
                int((miny - bng_ymax) / -cell_size),
            ),
            dims[1]: slice(int(minx / cell_size), int(maxx / cell_size)),
        },
    )
    return pdf


def _check_for_zarr(path: Path) -> bool:
    """Check if a zarr file exists in a given location.

    Args:
        path (Path): Directory to check for zarr file(s).

    Returns:
        bool: Whether the zarr exists.
    """
    try:
        open_dataset(path, engine="zarr")
        return True
    except FileNotFoundError:
        return False
    

def _graph_to_zarr(
    df_wrapper: DataFrameWrapper,
    zarr_path: str,
) -> None:
    """Write a Digraph source string to the attrs of a zarr.

    Args:
        self (DataFrameWrapper): _description_
        zarr_path (str): Directory containing a zarr file.
    """
    dataset = open_dataset(zarr_path, engine = 'zarr')
    dataset.attrs['DAG_source'] = df_wrapper.graph.source 
    dataset.close