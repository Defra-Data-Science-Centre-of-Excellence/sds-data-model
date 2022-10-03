from typing import Any, Dict, Generator, List, Optional, Tuple, TypeVar
from pathlib import Path
import pyspark.pandas as ps

from dataclasses import dataclass
from pyspark.pandas import  read_excel,DataFrame, Series

from pyspark_vector_files import read_vector_files
from pyspark_vector_files.gpkg import read_gpkg
#from pyspark.sql import DataFrame as SparkDataFrame
#from pyspark.sql import Column as SparkColumn


from sds_data_model.metadata import Metadata
from sds_data_model._vector import _get_metadata, _get_name # , _get_categories_and_dtypes,   _recode_categorical_strings
CategoryLookup = Dict[str, Dict[int, str]]
CategoryLookups = Dict[str, CategoryLookup]

# Wrapper is an upper bound for _Wrapper. Specifying bound means that _Wrapper will only be Wrapper or one of its subclasses. 
_Wrapper = TypeVar("_Wrapper", bound = "Wrapper")

@dataclass
class Wrapper:
    name: str
    data: DataFrame
    meta: Optional[Dict[str, Any]]
    category_lookups: Optional[CategoryLookups] = None
    #graph: Optional[DiGraph]

        
    @classmethod
    def from_files(
            cls: _Wrapper, 
            data_path: str,
            metadata_path: Optional[str] = None,
            name: Optional[str] = None,
            convert_to_categorical: Optional[List[str]] = None,
            category_lookups: Optional[CategoryLookups] = None,
            read_file_kwargs: Optional[Dict[str,Any]] = None
    ):
        
        
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
                data_path,  **read_file_kwargs).to_spark()
        elif suffix_data_path in file_reader_spark.keys():
            data = file_reader_spark[suffix_data_path](
            data_path, **read_file_kwargs)
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
            meta= metadata,
            category_lookups = category_lookups
        )