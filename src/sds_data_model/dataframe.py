from typing import Any, Dict, Optional, TypeVar
from typing_extensions import Self
from pathlib import Path
from inspect import ismethod, signature

from dataclasses import dataclass
from pyspark.pandas import  read_excel,DataFrame
from pyspark.sql import DataFrame as SparkDataFrame

from pyspark_vector_files import read_vector_files
from pyspark_vector_files.gpkg import read_gpkg
from pyspark.sql import SparkSession

from sds_data_model.metadata import Metadata
from sds_data_model._vector import _get_metadata, _get_name # , _get_categories_and_dtypes,   _recode_categorical_strings


spark = SparkSession.getActiveSession()

from logging import getLogger, INFO, StreamHandler, Formatter

# create logger and set level to info
logger = getLogger("sds")
logger.setLevel(INFO)

# create stream handler and set level to info
stream_handler = StreamHandler()
stream_handler.setLevel(INFO)

# create formatter
formatter = Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to stream handler
stream_handler.setFormatter(formatter)

# add stream handler to logger
logger.addHandler(stream_handler)

# DataFrameWrapper is an upper bound for _DataFrameWrapper. Specifying bound means that _DataFrameWrapper will only be DataFrameWrapper or one of its subclasses. 
_DataFrameWrapper = TypeVar("_DataFrameWrapper", bound = "DataFrameWrapper")

@dataclass
class DataFrameWrapper:
    name: str
    data: DataFrame
    meta: Optional[Dict[str, Any]]
    #graph: Optional[DiGraph]

        
    @classmethod
    def from_files(
            cls: _DataFrameWrapper, 
            data_path: str,
            metadata_path: Optional[str] = None,
            name: Optional[str] = None,
            read_file_kwargs: Optional[Dict[str,Any]] = None
    ) -> _DataFrameWrapper:

        
        if read_file_kwargs:
            read_file_kwargs =  read_file_kwargs
        else:
            read_file_kwargs = {}

        file_reader_pandas = { ".xlsx": read_excel,
                       ".xls": read_excel,
                       ".xlsm": read_excel,
                       ".xlsb": read_excel,
                       ".odf": read_excel,
                       ".ods": read_excel,
                       ".odt": read_excel}
      
        file_reader_spark = { ".csv": spark.read.csv,
                       ".json": spark.read.json,
                       ".parquet": spark.read.parquet}
        
        suffix_data_path = Path(data_path).suffix 
          
        
        if suffix_data_path in file_reader_pandas.keys():
             data = file_reader_pandas[suffix_data_path](
                data_path,  
                **read_file_kwargs
                ).to_spark()

        elif suffix_data_path in file_reader_spark.keys():
            data = file_reader_spark[suffix_data_path](
                data_path,
                **read_file_kwargs)
        elif suffix_data_path == ".gpkg":
            data = read_gpkg(
                data_path, 
                **read_file_kwargs
                )
        else:
            data = read_vector_files(
                data_path,
                **read_file_kwargs
            )
           
        
        metadata = _get_metadata(data_path = data_path,  metadata_path = metadata_path)
        
        _name = _get_name(
            name=name,
            metadata=metadata,
        )
        
        
        return cls(
            name=_name,
            data = data,
            meta= metadata
        )

    def call_method(
        self: Self, 
        method_name: str,
        *args,
        **kwargs,
    ) -> Optional[Self]:
        """Calls spark method specified by user on spark dataframe in wrapper, using user specified arguments. 
        THe function:
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

            >>> Wrapped = DataFrameWrapper.from_files(name = "National parks",  
                        data_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_traditional_orchards/format_SHP_traditional_orchards/LATEST_traditional_orchards/", 
                        read_file_kwargs = {'suffix':'.shp',  'ideal_chunk_size':1000})
        
            Limit number of rows to 5
            >>> Wrapped_small =  DataFrameWrapper.call_method(df_wrap,  method_name = 'limit', num = 8)

            Look at data
            >>> Wrapped_small.data.show()

        Args:
            self (Self): Spark DataFrame
            method_name (str): Name of method or property called by user

        Returns:
            Optional[Self]: Spark Dataframe or property output
        """
      
        attribute = getattr(self.data, method_name)
        
        if ismethod(attribute):
           
            sig = signature(attribute)
            
            arguments = sig.bind(*args, **kwargs).arguments
           
            formatted_arguments = ",".join(f"{key}={value}" for key, value in arguments.items())
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