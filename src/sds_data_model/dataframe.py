from typing import Any, Dict, Generator, List, Optional, Tuple, TypeVar
from typing_extensions import Self
from pathlib import Path
from inspect import ismethod, signature

from dataclasses import dataclass
from pyspark.pandas import  read_excel,DataFrame, Series
from pyspark.sql import DataFrame as SparkDataFrame

from pyspark_vector_files import read_vector_files
from pyspark_vector_files.gpkg import read_gpkg
from pyspark.sql import SparkSession
#from pyspark.sql import DataFrame as SparkDataFrame
#from pyspark.sql import Column as SparkColumn


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

    #@classmethod
    def call_method(
        self: Self, 
        method_name: str,
        *args,
        **kwargs,
    ) -> Optional[Self]:
      #assign method call to an object, so you can examine it before implementing anything
        attribute = getattr(self.data, method_name)
        #check if its a method
        if ismethod(attribute):
            #get the signature (what argument it takes, return type, etc. )
            sig = signature(attribute)
            #bind arguments to function arguments
            arguments = sig.bind(*args, **kwargs).arguments
            #create string to pass through graph generator
            formatted_arguments = ",".join(f"{key}={value}" for key, value in arguments.items())
            logger.info(f"Calling {attribute.__qualname__}({formatted_arguments})")
            #return value, so now no longer dealing with object, but calling method
            return_value = attribute(*args, **kwargs)
        else:
            #if not method, assume its a property (eg. crs)
            logger.info(f"Calling {attribute.__qualname__}")
            return_value = attribute
        #check if method_name returns a dataframe, if so update self.data attribute with that new data
        if isinstance(return_value, SparkDataFrame):
            self.data = return_value
            return self
        # if doesn't return a dataframe, (eg. called display), don't want to overwrite data attribute with nothing, so don't edit
        else:
            return return_value    