"""DataFrame wrapper class."""
from dataclasses import dataclass
from functools import partial
from inspect import ismethod, signature
from logging import INFO, Formatter, StreamHandler, getLogger, warning
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from bng_indexer import calculate_bng_index
from graphviz import Digraph
from pyspark.pandas import DataFrame as SparkPandasDataFrame
from pyspark.pandas import Series as SparkPandasSeries
from pyspark.pandas import read_excel
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import GroupedData, SparkSession
from pyspark.sql.functions import col, explode, udf
from pyspark.sql.types import ArrayType, StringType
from pyspark_vector_files import read_vector_files
from pyspark_vector_files.gpkg import read_gpkg

from sds_data_model._dataframe import (
    _bng_to_bounds,
    _check_for_zarr,
    _check_sparkdataframe,
    _create_dummy_dataset,
    _get_minimum_dtypes_and_nodata,
    _recode_column,
    _to_zarr_region,
)
from sds_data_model._vector import _get_metadata, _get_name
from sds_data_model.constants import BNG_XMAX, BNG_XMIN, BNG_YMAX, CELL_SIZE, OUT_SHAPE
from sds_data_model.graph import initialise_graph, update_graph
from sds_data_model.metadata import Metadata

spark = SparkSession.getActiveSession()

# create logger and set level to info
logger = getLogger("sds")
logger.setLevel(INFO)

# create stream handler and set level to info
stream_handler = StreamHandler()
stream_handler.setLevel(INFO)

# create formatter
formatter = Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# add formatter to stream handler
stream_handler.setFormatter(formatter)

# add stream handler to logger
logger.addHandler(stream_handler)

# DataFrameWrapper is an upper bound for _DataFrameWrapper.
# Specifying bound means that _DataFrameWrapper will only be
# DataFrameWrapper or one of its subclasses.
_DataFrameWrapper = TypeVar("_DataFrameWrapper", bound="DataFrameWrapper")


@dataclass
class DataFrameWrapper:
    """This is a thin wrapper around a Spark DataFrame.

    This class stores a Spark DataFrame alongside other objects which both enhance
    the richness of information associated with the data, and allow for increased
    flexibility in transforming it.

    Attributes:
        name (str): The name of the dataset.
        data (Union[SparkDataFrame, GroupedData]): Tabular data, optionally containing
        a geometry column.
        metadata (Metadata, optional): Object of class `Metadata` containing descriptive
        information relating to the dataset represented in `data`.
        lookup (Dict, optional): ???
        graph (Digraph, optional): Object of class `Digraph` containing nodes and edges
        relating to the source data and transformations that have taken place.

    Methods:
        from_files: Reads in data and converts it to a SparkDataFrame.
        call_method: Calls spark method specified by user on SparkDataFrame in wrapper.
        categorize: Maps an auto-generated or given dictionary onto provided columns.
        index: Adds a spatial index to data in a Spark DataFrame.
        to_zarr: Rasterises columns of `self.data` and writes them to `zarr`.

    Returns:
        _DataFrameWrapper: SparkDataFrameWrapper
    """

    name: str
    data: Union[SparkDataFrame, GroupedData]
    metadata: Optional[Metadata]
    lookup: Optional[Dict[str, Dict[Any, float]]]
    graph: Optional[Digraph]

    @classmethod
    def from_files(
        cls: Type[_DataFrameWrapper],
        data_path: str,
        metadata_path: Optional[str] = None,
        metadata_kwargs: Optional[Dict[str, Any]] = None,
        lookup: Optional[Dict[str, Dict[Any, float]]] = None,
        name: Optional[str] = None,
        read_file_kwargs: Optional[Dict[str, Any]] = None,
        spark: Optional[SparkSession] = None,
    ) -> _DataFrameWrapper:
        """Reads in data and converts it to a SparkDataFrame.

        A wide range of data can be read in with from_files. This includes vector
        data supported by GDAL drivers, multiple spreadsheet formats read with
        pandas.read_excel, and csvs, json and parquet files are handled by Spark.

        Examples:
            >>> from sds_data_model.dataframe import DataFrameWrapper
            >>> wrapped_shp = DataFrameWrapper.from_files(
                name = "National parks",
                data_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_national_parks/format_SHP_national_parks/LATEST_national_parks/",
                read_file_kwargs = {'suffix':'.shp'},
                metadata_path = "https://ckan.publishing.service.gov.uk/harvest/object/656c07d1-67b3-4bdb-8ab3-75e118a7cf14"
            )
            >>> wrapped_csv = DataFrameWrapper.from_files(
                name = "indicator_5__species_in_the_wider_countryside__farmland_1970_to_2020",
                data_path="dbfs:/mnt/lab/unrestricted/source_isr/dataset_england_biodiversity_indicators/format_CSV_england_biodiversity_indicators/LATEST_england_biodiversity_indicators/indicator_5__species_in_the_wider_countryside__farmland_1970_to_2020.csv",

                read_file_kwargs = {'header' :True}
            )

        Args:
            data_path (str): Path to data.
            metadata_path (Optional[str], optional): Path to metadata supplied by user. Defaults to None.
            metadata_kwargs (Optional[str]): Optional kwargs for metadata.
            lookup (Optional[Dict[str, Dict[Any, float]]]): Dictionary of `{column: value-map, ...}` for columns in the data. Not applied to the data. Defaults to None.
            name (Optional[str], optional): Name for data, either supplied by caller or obtained from metadata title. Defaults to None.
            read_file_kwargs (Optional[Dict[str,Any]], optional): Additional kwargs supplied by the caller, dependent on the function called. Defaults to None.
            spark(Optional[SparkSession]): Optional spark session.

        Returns:
            _DataFrameWrapper: SparkDataFrameWrapper
        """  # noqa: B950
        _spark = spark if spark else SparkSession.getActiveSession()

        if read_file_kwargs:
            read_file_kwargs = read_file_kwargs
        else:
            read_file_kwargs = {}

        file_reader_pandas = {
            ".xlsx": read_excel,
            ".xls": read_excel,
            ".xlsm": read_excel,
            ".xlsb": read_excel,
            ".odf": read_excel,
            ".ods": read_excel,
            ".odt": read_excel,
        }

        file_reader_spark: Dict[str, Callable] = {
            ".csv": _spark.read.options(header=True).csv,
            ".json": _spark.read.json,
            ".parquet": _spark.read.parquet,
        }

        suffix_data_path = Path(data_path).suffix

        if suffix_data_path in file_reader_pandas.keys():
            spark_pandas_data = file_reader_pandas[suffix_data_path](
                data_path, **read_file_kwargs
            )
            if isinstance(spark_pandas_data, SparkPandasDataFrame,) or isinstance(
                spark_pandas_data,
                SparkPandasSeries,
            ):
                data: SparkDataFrame = spark_pandas_data.to_spark()

        elif suffix_data_path in file_reader_spark.keys():
            data = file_reader_spark[suffix_data_path](data_path, **read_file_kwargs)
        elif suffix_data_path == ".gpkg":
            data = read_gpkg(data_path, **read_file_kwargs)
        else:
            data = read_vector_files(data_path, **read_file_kwargs)

        metadata = _get_metadata(data_path=data_path, metadata_path=metadata_path)

        _name = _get_name(
            name=name,
            metadata=metadata,
        )

        graph = initialise_graph(
            data_path=data_path,
            metadata_path=metadata_path,
            class_name="DataFrameWrapper",
        )

        return cls(name=_name, data=data, metadata=metadata, lookup=lookup, graph=graph)

    def call_method(
        self: _DataFrameWrapper,
        method_name: str,
        /,
        *args: Optional[Union[str, Sequence[str]]],
        **kwargs: Optional[Dict[str, Any]],
    ) -> _DataFrameWrapper:
        """Calls Spark method specified by user on Spark DataFrame in wrapper.

        The function does the following:
        1) assigns method call to attribute object, to examine it before implementing anything
        2) check if method_name is a method
        3) get the signature of method (what argument it takes, return type, etc.)
        4) bind arguments to function arguments
        5) create string to pass through graph generator
        6) return value, as have checked relevant information and can now call method
        7) if not a method, assume its a property (eg. CRS)
        8) check if method_name returns a dataframe, if so update self.data attribute with that new data
        9) if dataframe not returned, (eg. display called), return original data

        Examples:
            >>> from sds_data_model.dataframe import DataFrameWrapper
            >>> wrapped = DataFrameWrapper.from_files(name = "National parks",
                        data_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_traditional_orchards/format_SHP_traditional_orchards/LATEST_traditional_orchards/",
                        read_file_kwargs = {'suffix':'.shp'})

            Limit number of rows to 5
            >>> wrapped_small =  wrapped.call_method('limit', num = 5)
            Look at data
            >>> wrapped_small.call_method("show")

        Args:
            method_name (str): Name of method or property called by user.
            *args (Optional[Union[List[int], int]]): Additional non-keyword arguments provided by user.
            **kwargs (Optional[Dict[str, int]]): Additional keyword arguments provided by user.

        Returns:
            _DataFrameWrapper: The SparkDataFrameWrapper, updated if necessary
        """  # noqa: B950
        attribute = getattr(self.data, method_name)

        if ismethod(attribute):

            sig = signature(attribute)

            arguments = sig.bind(*args, **kwargs).arguments

            formatted_arguments = ",".join(
                f"{key}={value}" for key, value in arguments.items()
            )
            logger.info(f"Calling {attribute.__qualname__}({formatted_arguments})")

            return_value = attribute(*args, **kwargs)
        else:

            logger.info(f"Calling {attribute.__qualname__}")
            return_value = attribute

        if isinstance(return_value, SparkDataFrame) or isinstance(
            return_value, GroupedData
        ):
            self.data = return_value

            self.graph = update_graph(
                graph=self.graph,
                method=method_name,
                args=formatted_arguments,
                output_class_name="DataFrameWrapper",
            )

        return self

    def categorize(
        self: _DataFrameWrapper,
        columns: Sequence[str],
        lookup: Optional[Dict[str, Dict[Any, float]]] = None,
        spark: Optional[SparkSession] = None,
    ) -> _DataFrameWrapper:
        """Maps an auto-generated or given dictionary onto provided columns.

        This method is used to create a lookup (stored within the DataFrameWrapper)
        that is then used during the rasterisation process.

        Note: do not use `categorize` more than once in a given session, as the
        data from each run of the method update values inplace. If a new
        categorization is required then, re-create the DataFrameWrapper first.

        Examples:
            >>> from sds_data_model.dataframe import DataFrameWrapper
            >>> wrapped = DataFrameWrapper.from_files(name = "priority_habitats",
            data_path = '/dbfs/mnt/base/unrestricted/source_defra_data_services_platform',
            metadata_path = 'https://ckan.publishing.service.gov.uk/harvest/object/85e03bf0-4e95-4739-a5fa-21d60cf7f069',
            read_file_kwargs = {'pattern': 'dataset_priority_habitat_inventory_*/format_SHP_priority_habitat_inventory_*/LATEST_priority_habitat_inventory_*/PHI_v2_3_*',
            'suffix': '.shp'})

            Categorize by main habitat type
            >>> wrapped.categorize(['Main_Habit'])
            Look at lookup
            >>> wrapped.lookup

        Args:
            columns (Sequence[str]): Columns to map on.
            lookup (Optional[Dict[str, Dict[Any, float]]]): `{column: value-map}`
                dictionary to map. Defaults to {}.
            spark (Optional[SparkSession]): spark session.

        Returns:
            _DataFrameWrapper: SparkDataFrameWrapper

        """  # noqa: B950
        _spark = spark if spark else SparkSession.getActiveSession()

        self.data = _check_sparkdataframe(self.data)
        if not self.lookup:
            self.lookup = {}
        if not lookup:
            lookup = {}
        for column in columns:
            self.data, self.lookup[column] = _recode_column(
                self.data, column, lookup, spark=cast(SparkSession, _spark)
            )
        return self

    def index(
        self: _DataFrameWrapper,
        resolution: int = 100_000,
        how: str = "intersects",
        index_column_name: str = "bng_index",
        bounds_column_name: str = "bounds",
        geometry_column_name: str = "geometry",
        exploded: bool = True,
    ) -> _DataFrameWrapper:
        """Adds a spatial index to data in a Spark DataFrame.

        Calculates the grid index or indices for the geometrty provided in
        well-known binary format at a given resolution. An index is required
        for the rasterisation process executed in the `to_zarr` maethod. Executing
        this method will add relevant index columns to the dataframe stored within
        the DataFrameWrapper.

        Examples:
            >>> from sds_data_model.dataframe import DataFrameWrapper
            >>> wrapped = DataFrameWrapper.from_files(name = "priority_habitats",
            data_path = '/dbfs/mnt/base/unrestricted/source_defra_data_services_platform',
            metadata_path = 'https://ckan.publishing.service.gov.uk/harvest/object/85e03bf0-4e95-4739-a5fa-21d60cf7f069',
            read_file_kwargs = {'pattern': 'dataset_priority_habitat_inventory_*/format_SHP_priority_habitat_inventory_*/LATEST_priority_habitat_inventory_*/PHI_v2_3_*',
            'suffix': '.shp'})

            Index data.
            >>> wrapped.index(resolution = 100_000)

            Look at additional columns added to dataframe.
            >>> wrapped.data.dtypes

        Args:
            resolution (int): Resolution of British National Grid cell(s) to return. Defaults to 100_000.
            how (str): Indexing method of: bounding box, intersects (default), contains. Defaults to "intersects".
            index_column_name (str): Name of column in dataframe. Defaults to "bng_index".
            bounds_column_name (str): Name of column in dataframe. Defaults to "bounds".
            geometry_column_name (str): Name of column in dataframe. Defaults to "geometry".
            exploded (bool): ???. Defaults to True.

        Raises:
            ValueError: If `self.data` is an instance of `pyspark.sql.GroupedData`_
                instead of `pyspark.sql.DataFrame`_.

        Returns:
            _DataFrameWrapper: An indexed DataFrameWrapper.

        .. _`pyspark.sql.GroupedData`:
            https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.GroupedData.html

        .. _`pyspark.sql.DataFrame`:
            https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.html
        """  # noqa: B950
        if not isinstance(self.data, SparkDataFrame):
            data_type = type(self.data)
            raise ValueError(
                f"`self.data` must be a `pyspark.sql.DataFrame` not a {data_type}"
            )

        _partial_calculate_bng_index = partial(
            calculate_bng_index, resolution=resolution, how=how
        )

        _calculate_bng_index_udf = udf(
            _partial_calculate_bng_index,
            returnType=ArrayType(StringType()),
        )

        self.data = self.data.withColumn(
            index_column_name, _calculate_bng_index_udf(col(geometry_column_name))
        )

        if exploded:
            self.data = self.data.withColumn(
                index_column_name, explode(col(index_column_name))
            ).withColumn(bounds_column_name, _bng_to_bounds(col(index_column_name)))

        return self

    def to_zarr(
        self: _DataFrameWrapper,
        path: str,
        columns: Optional[List[str]] = None,
        nodata: Optional[Dict[str, float]] = None,
        index_column_name: str = "bng_index",
        geometry_column_name: str = "geometry",
        overwrite: bool = False,
        cell_size: int = CELL_SIZE,
        bng_xmin: int = BNG_XMIN,
        bng_xmax: int = BNG_XMAX,
        bng_ymax: int = BNG_YMAX,
        out_shape: Tuple[int, int] = OUT_SHAPE,
    ) -> None:
        """Rasterises columns of `self.data` and writes them to `zarr`.

        This function requires two additional columns:
        * A "bng_index" column containing the BNG index of the geometry in each row.
        * A "bounds" column containing the BNG bounds of the BNG index as a list in
            each row.

        Examples:
            >>> wrapper.to_zarr(
                path = "/path/to/file.zarr",
            )

        Args:
            path (str): Path to save the zarr file including file name.
            columns (Optional[List[str]]): Columns to rasterize. If `None`, a
                geometry mask will be generated. Defaults to None.
            nodata (Optional[Dict[str, float]]): Dictionary of `{column: nodata}`.
                Manual assignment of the nodata/fill value. Defaults to None.
            index_column_name (str): Name of the BNG index column. Defaults to
                "bng_index".
            geometry_column_name (str): Name of the geometry column. Defaults to
                "geometry".
            overwrite (bool): Overwrite existing zarr? Defaults to False.
            cell_size (int): The resolution of the cells in the DataArray. Defaults to
                CELL_SIZE.
            out_shape (Tuple[int, int]): The shape (height, width) of the DataArray.
                Defaults to OUT_SHAPE.
            bng_xmin (int): The minimum x value of the DataArray. Defaults to BNG_XMIN,
                the minimum x value of the British National Grid.
            bng_xmax (int): The maximum x value of the DataArray. Defaults to BNG_XMAX,
                the maximum x value of the British National Grid.
            bng_ymax (int): The maximum y value of the DataArray. Defaults to BNG_YMAX,
                the maximum y value of the British National Grid.

        Raises:
            ValueError: If `self.data` is not an instance of of `pyspark.sql.DataFrame`.
            ValueError: If `index_column_name` isn't in the dataframe.
            ValueError: If `geometry_column_name` isn't in the dataframe.
            ValueError: If Zarr file exists and overwrite set to False.
            ValueError: If column of type string is in `columns`.

        .. _`pyspark.sql.GroupedData`:
            https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.GroupedData.html

        .. _`pyspark.sql.DataFrame`:
            https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.html

        Returns:
        None.
        """  # noqa: B950
        self.data = _check_sparkdataframe(self.data)

        colnames = self.data.columns

        if index_column_name not in colnames:
            raise ValueError(f"{index_column_name} is not present in the data.")

        if geometry_column_name not in colnames:
            raise ValueError(f"{geometry_column_name} is not present in the data.")

        _path = Path(path)

        if _path.exists():

            if overwrite is False and _check_for_zarr(_path):
                raise ValueError(f"Zarr file already exists in {_path}.")

            if overwrite is True and _check_for_zarr(_path):
                warning("Overwriting existing zarr.")

        if columns:
            for column, _type in self.data.select(columns).dtypes:
                if "string" in _type:
                    raise ValueError(
                        f"Column `{column}` is of type string."
                        f"Cast or `categorize` to rasterize this column."
                    )

        dtype, nodata, self.lookup = _get_minimum_dtypes_and_nodata(
            sdf=self.data,
            columns=columns,
            nodata=nodata,
            lookup=self.lookup,
            mask_name=self.name,
        )

        _create_dummy_dataset(
            path=path,
            columns=columns,
            dtype=dtype,
            nodata=nodata,
            lookup=self.lookup,
            mask_name=self.name,
            metadata=self.metadata,
            cell_size=cell_size,
            bng_xmin=bng_xmin,
            bng_xmax=bng_xmax,
            bng_ymax=bng_ymax,
        )

        _partial_to_zarr_region = partial(
            _to_zarr_region,
            path=path,
            columns=columns,
            dtype=dtype,
            nodata=nodata,
            mask_name=self.name,
            cell_size=cell_size,
            out_shape=out_shape,
            bng_ymax=bng_ymax,
            geometry_column_name=geometry_column_name,
        )

        (
            self.data.groupby(index_column_name)
            .applyInPandas(
                _partial_to_zarr_region,
                self.data.schema,
            )
            .write.format("noop")
            .mode("overwrite")
            .save()
        )
