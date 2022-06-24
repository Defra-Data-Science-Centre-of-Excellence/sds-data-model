from dataclasses import dataclass
import inspect
import io
import json
from logging import INFO, info, basicConfig
import requests
from typing import Any, Dict, List, Optional, TypeVar, Union

from pandas import (DataFrame, Series,
                    read_csv, read_json, read_excel, read_parquet,
                    to_numeric)
from pathlib import Path
from urllib.parse import urlparse

from sds_data_model.metadata import Metadata

basicConfig(format="%(levelname)s:%(asctime)s:%(message)s", level=INFO)

_TableLayer = TypeVar("_TableLayer", bound="TableLayer")


def _find_closing_bracket(string):
    """Find the index of the closing bracket in a string."""
    flag = 1
    string_index = 1
    while flag > 0:
        if string[string_index] == "(":
            flag += 1
        elif string[string_index] == ")":
            flag -= 1
        string_index += 1
    return string_index - 1


def _get_function_input(func_name: str, frame: inspect.types.FrameType) -> str:
    """Get the input code arguments to a function as a string."""
    code_input = inspect.getouterframes(frame, 1)
    code_input = [f for f in code_input
                  if f'{func_name}(' in f.code_context[0]]
    code_input = code_input[0].code_context[0]
    code_input = code_input[len(func_name) + code_input.find(f'{func_name}('):]
    code_input = code_input[1:_find_closing_bracket(code_input)]
    return code_input


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
        cls: _TableLayer,
        data_path: str,
        data_kwargs: Optional[Dict[str, Any]] = {},
        metadata_path: Optional[str] = None,
        name: Optional[str] = None,
    ) -> _TableLayer:
        """Load tabular data from a file.

        Args:
            data_path (:obj:`str`): filepath or url for data.
            data_kwargs (:obj:`dict`, optional): Additional arguments to pass to data reader.
            metadata_path (:obj:`str`, optional): filepath or url for metadata.
            name (:obj:`str`, optional): Name of data.

        Returns:
            TableLayer object.

        Raises:
            NotImplementedError


    """
        file_reader = {".csv": read_csv,
                       ".json": read_json,
                       ".xlsx": read_excel,
                       ".xls": read_excel,
                       ".parquet": read_parquet}
        suffix = Path(data_path).suffix

        if suffix not in file_reader.keys():
            raise NotImplementedError("File format not supported.")

        if urlparse(data_path).scheme in ('http', 'https',):
            with requests.Session() as session:
                res = session.get(data_path)
                if suffix == '.csv':
                    df = file_reader[suffix](io.StringIO(res.content.decode('Windows-1252')),
                                             **data_kwargs)
                elif suffix in ['.xlsx', '.xls']:
                    df = file_reader[suffix](res.content, **data_kwargs)
                else:
                    raise NotImplementedError("File format not supported.")
        else:
            df = file_reader[suffix](data_path, **data_kwargs)

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
        """Select columns from TableLayer DataFrame"""
        select_output = TableLayer(
            name=self.name,
            df=self.df.loc[:, columns],
            metadata=self.metadata
        )
        info(f"Selected columns: {columns}.")
        return select_output

    def where(self: _TableLayer, condition: Series) -> _TableLayer:
        """Filter rows from TableLayer DataFrame using pandas Series object."""
        frame = inspect.currentframe()
        try:
            condition_str = _get_function_input("where", frame)
        finally:
            del frame
        where_output = TableLayer(
            name=self.name,
            df=self.df.loc[condition, :],
            metadata=self.metadata
        )
        info(f"Filtered rows using condition: {condition_str}.")
        return where_output

    def query(self: _TableLayer, expression: str) -> _TableLayer:
        """Filter rows from TableLayer DataFrame using arithmetic expression."""
        query_output = TableLayer(
            name=self.name,
            df=self.df.query(expression),
            metadata=self.metadata
        )
        info(f"Filtered rows using expression: {expression}.")
        return query_output

    def join(self: _TableLayer,
             other: _TableLayer,
             how: str = "left",
             kwargs: Dict[str, Any] = None
             ) -> _TableLayer:
        """Join two TableLayers using pandas merge method."""
        join_output = TableLayer(
            name=self.name,
            df=self.df.merge(right=other.df, how=how, **kwargs),
            metadata=self.metadata
        )
        info(f"Joined to {other.name}.")
        return join_output

    def replace(self: _TableLayer,
                column: str,
                pattern: str,
                replacement: str,
                kwargs: Dict[str, Any] = {}
                ) -> _TableLayer:
        """Replace characters in a string column."""
        replace_output = TableLayer(
            name=self.name,
            df=self.df.assign(**{column: (self.df[column].str
                                          .replace(pattern,
                                                   replacement,
                                                   **kwargs))}),
            metadata=self.metadata
        )
        info(f"Replaced {pattern} with {replacement} in column: {column}")
        return replace_output

    def to_numerical(self: _TableLayer,
                     column: str,
                     kwargs: Dict[str, Any] = {'errors': 'coerce'}
                     ) -> _TableLayer:
        """Cast a field to a numerical datatype."""
        numeric_output = TableLayer(
            name=self.name,
            df=self.df.assign(**{column: to_numeric(self.df[column], **kwargs)}),
            metadata=self.metadata
        )
        info(f"Cast column '{column}' to numeric datatype: {numeric_output.df[column].dtype}")
        return numeric_output
