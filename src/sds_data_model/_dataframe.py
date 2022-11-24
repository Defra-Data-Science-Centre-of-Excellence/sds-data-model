"""Private functions for the DataFrame wrapper class."""
from dataclasses import asdict
from json import load
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from affine import Affine
from bng_indexer import wkt_from_bng
from geopandas import GeoDataFrame, GeoSeries
from itertools import chain
from numpy import arange, array, can_cast, full, isnan, nan, ndarray
from pandas import DataFrame as PandasDataFrame
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import col, create_map, lit, max as _max, min as _min, udf
from pyspark.sql.types import ArrayType, FloatType
from rasterio.env import GDALVersion
from rasterio.features import rasterize
from rioxarray.rioxarray import affine_to_coords
from shapely.wkt import loads
from xarray import DataArray, Dataset, merge, open_dataset

from sds_data_model.constants import (
    BNG_XMAX,
    BNG_XMIN,
    BNG_YMAX,
    CELL_SIZE,
    OUT_SHAPE,
)
from sds_data_model.metadata import Metadata


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


_GDAL_AT_LEAST_35 = GDALVersion.runtime().at_least("3.5")

dtype_fill_value = {
    "uint8": 255,
    "int16": 32767,
    "uint16": 65535,
    "int32": 2147483647,
    "uint32": 4294967295,
    "int64": 9223372036854775807,
    "float32": nan,
    "float64": nan,
}


def _get_minimum_dtype(
    values,
) -> str:
    """
    Derived from `rasterio.dtypes.get_minimum_dtype`

    Args:
        values (_type_): _description_

    Raises:
        ValueError: _description_
        ValueError: _description_

    Returns:
        str: _description_
    """
    if not isinstance(values, ndarray):
        values = array(values)
    if values.dtype.kind == "b":
        return "bool"
    if values.dtype.kind in ("i", "u"):
        min_value = values.min()
        max_value = values.max()
        if min_value >= 0:
            if max_value <= 255:
                return "uint8"
            elif max_value <= 65535:
                return "uint16"
            elif max_value <= 4294967295:
                return "uint32"
            if not _GDAL_AT_LEAST_35:
                raise ValueError("Values out of range for supported dtypes")
            return "uint64"
        elif min_value >= -32768 and max_value <= 32767:
            return "int16"
        elif min_value >= -2147483648 and max_value <= 2147483647:
            return "int32"
        if not _GDAL_AT_LEAST_35:
            raise ValueError("Values out of range for supported dtypes")
        return "int64"
    else:
        values = values[~isnan(values)]
        min_value = values.min()
        max_value = values.max()
        if min_value >= -3.4028235e38 and max_value <= 3.4028235e38:
            return "float32"
        return "float64"


def _map_dictionary_on_column(
    sdf: SparkDataFrame,
    column: str,
    lookup: Dict[Any, float],
) -> SparkDataFrame:
    _map = create_map([lit(x) for x in chain(*lookup.items())])
    return sdf.withColumn(column, _map[col(column)])


def _create_category_lookup(
    self, column: str
) -> Tuple[str, float, Dict[str, float]]:
    unique = self.data.select(column).distinct()
    length = unique.count()
    lookup = dict(zip(unique.rdd.flatMap(lambda x: x).collect(), range(length)))
    dtype = _get_minimum_dtype(length)
    self.data = _map_dictionary_on_column(self.data, column, lookup)

    lookup["No data"] = nodata = dtype_fill_value[dtype]

    return dtype, nodata, lookup


def _get_minimum_dtype_from_column(
    sdf: SparkDataFrame,
    column: str,
) -> str:
    return _get_minimum_dtype(
        sdf.select(column).distinct().dropna().rdd.flatMap(lambda x: x).collect()
    )


def _get_minimum_dtype_and_fill(
    sdf: SparkDataFrame,
    column: str,
) -> Tuple[str, float]:
    dtype = _get_minimum_dtype_from_column(sdf, column)
    _max_value = sdf.dropna().select(_max(sdf[column])).first()[0]

    if _max_value == dtype_fill_value[dtype]:
        _next_dtype = lambda _type: list(dtype_fill_value.items())[
            list(dtype_fill_value.keys()).index(_type) + 1
        ]
        dtype, fill = _next_dtype(dtype)
        _min_value = sdf.select(_min(sdf[column])).first()[0]
        while not can_cast(_min_value, dtype):
            dtype, fill = _next_dtype(dtype)
    else:
        fill = dtype_fill_value[dtype]

    return dtype, fill


def _get_minimum_dtype_from_fill(
    sdf: SparkDataFrame,
    column: str,
    nodata: float,
) -> str:
    dtype = _get_minimum_dtype_from_column(sdf, column)
    while not can_cast(nodata, dtype):
        dtype = list(dtype_fill_value.keys())[
            list(dtype_fill_value.keys()).index(dtype) + 1
        ]
    return dtype


def _process_category_lookup(
    self,
    column: str,
    nodata: Dict[str, float],
    lookup: Dict[str, Dict[Any, float]],
) -> Tuple[str, float, Dict[Any, float]]:
    dtype = _get_minimum_dtype(list(lookup[column].values()))
    _nodata = (
        lookup[column].pop("No data") if "No data" in lookup[column] else nodata[column]
    )
    self.data = _map_dictionary_on_column(self.data, column, lookup[column])
    lookup[column]["No data"] = _nodata
    return dtype, _nodata, lookup[column]


def _get_minimum_dtypes_and_fill_vals(
    self,
    columns: Optional[Sequence],
    categorical_columns: Optional[Sequence],
    dtype: Dict[str, str],
    nodata: Dict[str, float],
    lookup: Dict[str, Dict[Any, float]],
    dataset_name: str,
) -> Tuple[List[str], Dict[str, str], Dict[str, float], Dict[str, Dict[Any, float]]]:
    _columns, _dtype, _nodata, _lookup = [], {}, {}, {}
    if not all((columns, categorical_columns)):
        _dtype[dataset_name] = "uint8"
        _nodata[dataset_name] = 0
    else:
        if columns:
            for column in columns:
                _columns.append(column)
                if column in dtype:
                    _dtype[column] = dtype[column]
                    _nodata[column] = nodata[column]
                elif nodata:
                    _dtype[column] = _get_minimum_dtype_from_fill(
                        self.data, column, nodata[column]
                    )
                    _nodata[column] = nodata[column]
                else:
                    _dtype[column], _nodata[column] = _get_minimum_dtype_and_fill(
                        self.data, column
                    )
        if categorical_columns:
            for column in categorical_columns:
                _columns.append(column)
                if column in lookup:
                    (
                        _dtype[column],
                        _nodata[column],
                        _lookup[column],
                    ) = _process_category_lookup(self, column, nodata, lookup)
                elif column:
                    (
                        _dtype[column],
                        _nodata[column],
                        _lookup[column],
                    ) = _create_category_lookup(self, column)
    return _columns, _dtype, _nodata, _lookup


def _create_empty_dataset(
    column: str,
    nodata: float,
    lookup: Dict[str, Dict[Any, float]],
    metadata: Dict,
    dtype: str,
    dims: Tuple[str, str],
    height: int,
    width: int,
) -> Dataset:
    attrs = {**metadata, "No data": nodata}
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
    columns: List,
    dtype: Dict[str, str],
    nodata: Dict[str, float],
    lookup: Dict[str, Dict[Any, float]],
    dataset_name: str,
    metadata: Optional[Metadata] = None,
    cell_size: int = CELL_SIZE,
    bng_xmin: int = BNG_XMIN,
    bng_xmax: int = BNG_XMAX,
    bng_ymax: int = BNG_YMAX,
) -> None:
    """A dummy Dataset. It's metadata is used to create the initial `zarr` store.

    See `Appending to existing Zarr stores`_ for more details.

    Examples:
        >>> d_dataset = _create_dummy_dataset(
            path = "/path/to/dummy.zarr"
        )

        >>> d_dataset
        Delayed('_finalize_store-31bc6052-52db-49e8-bc87-fc8f7c6801ed')

    Args:
        path (str): Path to save the zarr file including file name.
        metadata (Optional[str], optional): Metadata object relating to data.
        dtype (str):
        cell_size (int): The resolution of the cells in the DataArray. Defaults to
            CELL_SIZE.
        bng_xmin (int): The minimum x value of the British National Grid.
            Defaults to BNG_XMIN.
        bng_xmax (int): The maximum x value of the British National Grid.
            Defaults to BNG_XMAX.
        bng_ymax (int): The maximum y value of the British National Grid.
            Defaults to BNG_YMAX.

    .. _`Appending to existing Zarr stores`:
        https://docs.xarray.dev/en/stable/user-guide/io.html#appending-to-existing-zarr-stores  # noqa: B950
    """
    _metadata = asdict(metadata) if metadata else {}
    dims = ("northings", "eastings")
    height = int(bng_ymax / cell_size)
    width = int(bng_xmax / cell_size)
    transform = Affine(cell_size, 0, bng_xmin, 0, -cell_size, bng_ymax)

    if not columns:
        columns = [dataset_name]
    dataset = merge(
        _create_empty_dataset(
            column,
            nodata[column],
            lookup,
            _metadata,
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
    dataset.attrs.clear()
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
    dataset_name: str,
    cell_size: int = CELL_SIZE,
    out_shape: Tuple[int, int] = OUT_SHAPE,
    bng_ymax: int = BNG_YMAX,
    geometry_column_name: str = "geometry",
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
        cell_size (int): The resolution of the cells in the DataArray. Defaults to
            CELL_SIZE.
        out_shape (Tuple[int, int]): The shape (height, width) of the DataArray.
            Defaults to OUT_SHAPE.
        bng_ymax (int): The maximum y value of the British National Grid.
            Defaults to BNG_YMAX.
        geometry_column_name (str): The name of the geometry column in the
            Pandas DataFrame. Defaults to "geometry".

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

    if not columns:
        columns = [dataset_name]
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
