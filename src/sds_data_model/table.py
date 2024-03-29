"""Tabular data wrapper class."""
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, TypeVar, Union

from pandas import DataFrame, Series, read_csv, read_excel, read_json, read_parquet

from sds_data_model._table import _update_datatypes
from sds_data_model.metadata import Metadata

_TableLayer = TypeVar("_TableLayer", bound="TableLayer")


@dataclass
class TableLayer:
    """Class for simple rectangular table data.

    Attributes:
        name (str): Name of data.
        df (DataFrame): pandas dataframe storing data.
        metadata (Metadata): metadata for data.


    """

    name: str
    df: DataFrame
    metadata: Metadata

    @classmethod
    def from_file(
        cls: Type[_TableLayer],
        data_path: str,
        data_kwargs: Optional[Dict[str, Any]] = None,
        metadata_path: Optional[str] = None,
        name: Optional[str] = None,
    ) -> _TableLayer:
        """Load data from a file.

        Args:
            data_path (str): filepath or url for data.
            data_kwargs (Dict[str, Any], optional): Additional arguments to pass
                to data reader. Defaults to None.
            metadata_path (str, optional): filepath or url for metadata. Defaults to
                None.
            name (str, optional): Name of data. Defaults to None.

        Raises:
            NotImplementedError: # TODO

        Returns:
            _TableLayer: TableLayer object.
        """
        file_reader = {
            ".csv": read_csv,
            ".json": read_json,
            ".xlsx": read_excel,
            ".xls": read_excel,
            ".xlsm": read_excel,
            ".xlsb": read_excel,
            ".odf": read_excel,
            ".ods": read_excel,
            ".odt": read_excel,
            ".parquet": read_parquet,
        }
        suffix = Path(data_path).suffix

        if suffix not in file_reader.keys():
            raise NotImplementedError(f"File format '{suffix}' not supported.")

        df = file_reader[suffix](data_path, **data_kwargs)

        # update data types so they are not the default dtypes when read in using
        # pandas. Pandas default to the largest dtype (eg. int64) which takes up
        # unnecessary space
        df = _update_datatypes(df)

        if not metadata_path:
            try:
                # This is the default for csvw
                metadata = json.load(open(f"{data_path}-metadata.json"))
            except FileNotFoundError:
                # If no metadata file is available
                metadata = None
        else:
            metadata = Metadata.from_file(metadata_path)

        _name = name if name else metadata["title"]

        return cls(
            name=_name,
            df=df,
            metadata=metadata,
        )

    def select(self: _TableLayer, columns: Union[str, List[str]]) -> _TableLayer:
        """Select columns from TableLayer DataFrame.

        Examples:
            # TODO

        Args:
            columns (Union[str, List[str]]): # TODO

        Returns:
            _TableLayer: # TODO
        """
        self.df = self.df.loc[:, columns]
        return self

    def where(self: _TableLayer, condition: Series) -> _TableLayer:
        """Filter rows from TableLayer DataFrame using pandas Series object.

        Examples:
            # TODO

        Args:
            condition (Series): # TODO

        Returns:
            _TableLayer: # TODO
        """
        self.df = self.df.loc[condition, :]
        return self

    def join(
        self: _TableLayer,
        other: _TableLayer,
        how: str = "left",
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> _TableLayer:
        """Join two TableLayers using pandas merge method.

        Examples:
            # TODO

        Args:
            other (_TableLayer): # TODO
            how (str): # TODO. Defaults to "left".
            kwargs (Dict[str, Any], optional): # TODO. Defaults to None.

        Returns:
            _TableLayer: # TODO
        """
        self.df = self.df.merge(right=other.df, how=how, **kwargs)
        return self
