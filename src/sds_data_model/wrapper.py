"""Wrapper class for pandas-on-spark dataframe."""
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Type, TypeVar

from affine import Affine
from dask.delayed import Delayed
from geopandas import GeoDataFrame
from numpy import number, uint8
from numpy.typing import NDArray
from pathlib import Path
#from pandas import DataFrame, Series
from pyspark.pandas import read_csv, read_excel, read_json, read_parquet, DataFrame, Series
import pyspark.pandas as ps
from shapely.geometry import box
from xarray import DataArray, Dataset, merge

from pyspark_vector_files import read_vector_files


from sds_data_model._wrapper import (
    _check_layer_projection,
    _from_delayed_to_data_array,
    _from_file,
    _get_categories_and_dtypes,
    _get_gpdf,
    _get_info,
    _get_mask,
    _get_metadata,
    _get_name,
    _get_schema,
    _join,
    _recode_categorical_strings,
    _select,
    _to_raster,
    _where,
)
from sds_data_model.constants import (
    BBOXES,
    CELL_SIZE,
    OUT_SHAPE,
    BoundingBox,
    CategoryLookups,
    Schema,
)
from sds_data_model.logger import log
from sds_data_model.metadata import Metadata

    
_Wrapper = TypeVar("_Wrapper", bound = "Wrapper")

@dataclass
class Wrapper:
    name: str
    data: DataFrame
    metadata: Optional[Dict[str, Any]]
    category_lookups: Optional[CategoryLookups] = None
    #graph: Optional[DiGraph]

        
    @classmethod
    def from_files(
            cls: _Wrapper, 
            data_path: str,
           # data_kwargs: Optional[Dict[str, Any]] = {},
          
            #suffix: Optional[str] = None,
            metadata_path: Optional[str] = None,
            name: Optional[str] = None,
            convert_to_categorical: Optional[List[str]] = None,
            category_lookups: Optional[CategoryLookups] = None,
            **read_vector_file_kwargs: Optional[Dict[str,Any]],
           # **read_file_kwargs: Optional[Dict[str, Any]]
    ):
        
        
        file_reader_pandas = { ".csv": read_csv,
                       ".json": read_json,
                       ".parquet": read_parquet,
                        ".xlsx": read_excel,
                       ".xls": read_excel,
                       ".xlsm": read_excel,
                       ".xlsb": read_excel,
                       ".odf": read_excel,
                       ".ods": read_excel,
                       ".odt": read_excel}
      
        
        suffix_data_path = Path(data_path).suffix 
          
        
        if suffix_data_path in file_reader_pandas.keys():
             
            data = file_reader_pandas[suffix_data_path]( data_path)
        else:
            data = read_vector_files(
                data_path,
              **read_vector_file_kwargs
            ).to_pandas_on_spark()
                
       
        
        metadata = _get_metadata(data_path = data_path,  metadata_path = metadata_path)
        
        _name = _get_name(
            name=name,
            metadata=metadata,
        )
        
        if convert_to_categorical:
            category_lookups, dtype_lookup = _get_categories_and_dtypes(
                data_path=data_path,
                convert_to_categorical=convert_to_categorical,
                **read_vector_file_kwargs,
            )
            for column in convert_to_categorical:
                    data = _recode_categorical_strings(
                       df = data, column = column,
                        category_lookups = category_lookups
                    )

        return cls(
            name=_name,
            data = data,
            metadata= metadata,
            category_lookups = category_lookups
        )