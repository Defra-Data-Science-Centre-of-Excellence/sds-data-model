from typing import Any, Dict, Optional,  Type, TypeVar
from pathlib import Path

from dataclasses import dataclass
from pyspark.pandas import  read_excel,DataFrame, Series

from pyspark_vector_files import read_vector_files
from pyspark_vector_files.gpkg import read_gpkg
from pyspark.sql import SparkSession
#from pyspark.sql import DataFrame as SparkDataFrame
#from pyspark.sql import Column as SparkColumn


from sds_data_model.metadata import Metadata
from sds_data_model._vector import _get_metadata, _get_name # , _get_categories_and_dtypes,   _recode_categorical_strings


spark = SparkSession.getActiveSession()

# DataFrameWrapper is an upper bound for _DataFrameWrapper. Specifying bound means that _DataFrameWrapper will only be DataFrameWrapper or one of its subclasses. 
_DataFrameWrapper = TypeVar("_DataFrameWrapper", bound = "DataFrameWrapper")

@dataclass
class DataFrameWrapper:
    name: str
    data: DataFrame
    metadata: Optional[Dict[str, Any]]
    #graph: Optional[DiGraph]

        
    @classmethod
    def from_files(
            cls: Type[_DataFrameWrapper], 
            data_path: str,
            metadata_path: Optional[str] = None,
            name: Optional[str] = None,
            read_file_kwargs: Optional[Dict[str,Any]] = None
    ) -> _DataFrameWrapper:
        """Reads in data with a range of file types, and converts it to a spark dataframe, wrapped with associated metadata
        
        Examples:
        >>> from sds_data_model.dataframe import DataFrameWrapper

        >>> wrapped_shp = DataFrameWrapper.from_files(name = "National parks",                               
                                        data_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_national_parks/format_SHP_national_parks/LATEST_national_parks/",
                                        read_file_kwargs = {'suffix':'.shp',  'ideal_chunk_size':1000},
                                        metadata_path =  "https://ckan.publishing.service.gov.uk/harvest/object/656c07d1-67b3-4bdb-8ab3-75e118a7cf14"
                                       )

        >>> wrapped_csv = check_csv = DataFrameWrapper.from_files( name = "aes30", 
                                        data_path="dbfs:/mnt/lab/unrestricted/james.duffy@defra.gov.uk/aes30_in_aonbs.csv", 
                                        read_file_kwargs = {'header' :True}
                                       )

        Args:
            cls (_DataFrameWrapper): DataFrameWrapper class , defined above
            data_path (str): path to data, 
            metadata_path (Optional[str], optional): _description_. Defaults to None.
            name (Optional[str], optional): Name for data, either supplied by caller or obtained from metadata title. Defaults to None.
            read_file_kwargs (Optional[Dict[str,Any]], optional): Additional kwargs supplied by the caller, dependent on the function called. Defaults to None.

        Returns:
            _DataFrameWrapper: _description_

        """
        
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
            metadata= metadata
        )

        