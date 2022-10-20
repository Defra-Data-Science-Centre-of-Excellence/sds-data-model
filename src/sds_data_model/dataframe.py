from dataclasses import dataclass
from functools import partial
from inspect import ismethod, signature
from logging import INFO, Formatter, StreamHandler, getLogger
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Type, TypeVar, List, Tuple

from pandas import DataFrame as PandasDataFrame
from pyspark.pandas import DataFrame as SparkPandasDataFrame
from pyspark.pandas import Series as SparkPandasSeries
from pyspark.pandas import read_excel
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf
from pyspark.sql.types import ArrayType, FloatType, StringType
from pyspark_vector_files import read_vector_files
from pyspark_vector_files.gpkg import read_gpkg
from typing_extensions import Self
from shapely.wkt import loads
from bng_indexer import calculate_bng_index, wkt_from_bng

from sds_data_model._dataframe import _create_dummy_dataset, _to_zarr_region, _bng_to_bounds
from sds_data_model._vector import _get_metadata, _get_name
from sds_data_model.metadata import Metadata

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
    name: str
    data: SparkDataFrame
    metadata: Optional[Metadata]
    # graph: Optional[DiGraph]

    @classmethod
    def from_files(
        cls: Type[_DataFrameWrapper],
        data_path: str,
        metadata_path: Optional[str] = None,
        metadata_kwargs: Optional[Dict[str, Any]] = None,
        name: Optional[str] = None,
        read_file_kwargs: Optional[Dict[str, Any]] = None,
        spark: Optional[SparkSession] = None,
    ) -> _DataFrameWrapper:
        """Reads in data with a range of file types, and converts it to a spark
        dataframe, wrapped with associated metadata

        Examples:
        >>> from sds_data_model.dataframe import DataFrameWrapper
        >>> wrapped_shp = DataFrameWrapper.from_files(name = "National parks",
                                        data_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_national_parks/format_SHP_national_parks/LATEST_national_parks/",
                                        read_file_kwargs = {'suffix':'.shp'},
                                        metadata_path =  "https://ckan.publishing.service.gov.uk/harvest/object/656c07d1-67b3-4bdb-8ab3-75e118a7cf14"
                                       )
        >>> wrapped_csv =  DataFrameWrapper.from_files(
                                        name = "indicator_5__species_in_the_wider_countryside__farmland_1970_to_2020",
                                        data_path="dbfs:/mnt/lab/unrestricted/source_isr/dataset_england_biodiversity_indicators/format_CSV_england_biodiversity_indicators/LATEST_england_biodiversity_indicators/indicator_5__species_in_the_wider_countryside__farmland_1970_to_2020.csv",
                                        read_file_kwargs = {'header' :True}
                                       )
        Args:
            cls (_DataFrameWrapper): DataFrameWrapper class , defined above
            data_path (str): Path to data,
            metadata_path (Optional[str], optional): Path to metadata supplied by user. Defaults to None.
            name (Optional[str], optional): Name for data, either supplied by caller or obtained from metadata title. Defaults to None.
            read_file_kwargs (Optional[Dict[str,Any]], optional): Additional kwargs supplied by the caller, dependent on the function called. Defaults to None.
        Returns:
            _DataFrameWrapper: SparkDataFrameWrapper
        """
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
            ".csv": _spark.read.csv,
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
                data: SparkDataFrame = data.to_spark()

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

        return cls(name=_name, data=data, metadata=metadata)

    def call_method(
        self: _DataFrameWrapper,
        method_name: str,
        /,
        *args,
        **kwargs,
    ) -> Optional[_DataFrameWrapper]:
        """Calls spark method specified by user on SparkDataFrame in wrapper.

        The function:
        1) assign method call to attribute object, so you can examine it before implementing anything
        2) check if method_name is a method
        3) get the signature of method (what argument it takes, return type, etc. )
        4) bind arguments to function arguments
        5) create string to pass through graph generator
        6) return value, as have checked relevant information and can now call method
        7) if not method, assume its a property (eg. crs)
        8) check if method_name returns a dataframe, if so update self.data attribute with that new data
        9) if doesn't return a dataframe, (eg. called display), don't want to overwrite data attribute with nothing, so return original data

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
            self (_DataFrameWrapper): Spark DataFrameWrapper
            method_name (str): Name of method or property called by user
        Returns:
            Optional[_DataFrameWrapper]: Updated SparkDataFrameWrapper or property output
        """

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

        if isinstance(return_value, SparkDataFrame):
            self.data = return_value
            return self

        else:
            return return_value

    def to_zarr(
        self: Self,
        path: str,
        data_array_name: str,
        index_column_name: str = "bng_index",
        geometry_column_name: str = "geometry",
    ) -> None:
        """Reads in data as a Spark DataFrame. A dummy dataset of the BNG is created and written to zarr which is then overwritten
        as the Spark DataFrame is converted to a mask then a Dataset.
        This function assumes that the dataframe contains a column with the BNG 100km grid reference
        and a column containing the bounds of the BNG grid refernce system as a list named as "bounds"

        Args:
            self (Self): _description_
            path (str): _description_
            data_array_name (str): _description_
            index_column_name (str, optional): _description_. Defaults to "bng_index".
            geometry_column_name (str, optional): _description_. Defaults to "geometry".

        Returns:
            _type_: _description_
        """

        _create_dummy_dataset(
            path=path,
            data_array_name=data_array_name,
        )
        _partial_to_zarr_region = partial(
            _to_zarr_region,
            data_array_name=data_array_name,
            path=path,
        )
        return (
            self.data.groupby(index_column_name)
            .applyInPandas(
                _partial_to_zarr_region,
                self.data.schema,
            )
            .write.format("noop")
            .mode("overwrite")
            .save()
        )
        
    def index(
        self: Self,
        resolution: int,
        how: str = "intersects",
        index_column_name: str = "bng",
        bounds_column_name: str = "bounds",
        geometry_column_name: str = "geometry",
        exploded: bool = True,
    ) -> _DataFrameWrapper:
        """_summary_

        Args:
            self (Self): _description_
            resolution (int): _description_
            how (str, optional): _description_. Defaults to "intersects".
            index_column_name (str, optional): _description_. Defaults to "bng".
            bounds_column_name (str, optional): _description_. Defaults to "bounds".
            geometry_column_name (str, optional): _description_. Defaults to "geometry".
            exploded (bool, optional): _description_. Defaults to True.

        Returns:
            _DataFrameWrapper: _description_
        """        
        _partial_calculate_bng_index = partial(
            calculate_bng_index, resolution=resolution, how=how
            )
        
        _calculate_bng_index_udf = udf(
            _partial_calculate_bng_index,
            returnType=ArrayType(StringType()),
            )
        
        self.data = self.data.withColumn(
            index_column_name, 
            _calculate_bng_index_udf(col(geometry_column_name))
            )
        
        if exploded:
            self.data = self.data.withColumn(
                index_column_name, 
                explode(col(index_column_name))
                ).withColumn(bounds_column_name, _bng_to_bounds(col(index_column_name)))
        
        return self