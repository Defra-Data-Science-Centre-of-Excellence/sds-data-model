from dataclasses import dataclass
import inspect
import io
import json
from logging import INFO, info, basicConfig
import requests
from typing import Any, Dict, List, Optional, TypeVar, Union

from pandas import DataFrame, Series, read_csv, read_json, read_excel, read_parquet
from pathlib import Path
from urllib.parse import urlparse

from sds_data_model.metadata import Metadata

basicConfig(format="%(levelname)s:%(asctime)s:%(message)s", level=INFO)

_TableLayer = TypeVar("_TableLayer", bound="TableLayer")

def _get_function_input(func_name: str, frame: inspect.types.FrameType) -> str:
    code_input = inspect.getouterframes(frame, 1)
    code_input = [f for f in code_input if f'{func_name}(' in f.code_context[0]]
    code_input = code_input[0].code_context[0]
    code_input = code_input[6 + code_input.find(f'{func_name}('):code_input.rfind(')')]
    return code_input

@dataclass
class TableLayer:
    name: str
    df: DataFrame
    metadata: Metadata

    @classmethod
    def from_csvw(
        cls: _TableLayer,
        data_path: str,
        data_kwargs: Dict[str, Any],
        metadata_path: Optional[str] = None,
        name: Optional[str] = None,
    ) -> _TableLayer:
        file_reader = {".csv": read_csv,
                       ".json": read_json,
                       ".xlsx": read_excel,
                       ".xls": read_excel,
                       ".parquet": read_parquet}
        suffix = Path(data_path).suffix
        if urlparse(data_path).scheme in ('http', 'https',):
            with requests.Session() as session:
                res = session.get(data_path)
                if suffix == '.csv':
                    df = file_reader[suffix](io.StringIO(res.content.decode('Windows-1252')),
                                             **data_kwargs)
                else:
                    df = file_reader[suffix](res.content, **data_kwargs)

        else:
            df = file_reader[suffix](data_path, **data_kwargs)

        if not metadata_path:
            try:
                # This is the default for csvw
                metadata = json.load(f"{data_path}-metadata.json")
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
        select_output = TableLayer(
            name=self.name,
            df=self.df.loc[:, columns],
            metadata=self.metadata
        )
        info(f"Selected columns: {columns}.")
        return select_output

    def where(self: _TableLayer, condition: Series) -> _TableLayer:
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
        join_output = TableLayer(
            name=self.name,
            df=self.df.merge(right=other.df, how=how, **kwargs),
            metadata=self.metadata
        )
        info(f"Joined to {other.name}.")
        return join_output
