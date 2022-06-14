from dataclasses import dataclass
import inspect
import io
import json
from logging import INFO, info, basicConfig
import requests
from typing import Any, Dict, Generator, List, Optional, TypeVar, Tuple, Union

from pandas import DataFrame, Series, read_csv
from urllib.parse import urlparse

from sds_data_model.metadata import Metadata

basicConfig(format="%(levelname)s:%(asctime)s:%(message)s", level=INFO)

_TableLayer = TypeVar("_TableLayer", bound="TableLayer")


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
        if urlparse(data_path).scheme in ('http', 'https',):
            with requests.Session() as session:
                res = session.get(data_path)
                df = read_csv(io.StringIO(res.content.decode('Windows-1252')),
                              **data_kwargs)
        else:
            df = read_csv(data_path, **data_kwargs)

        if not metadata_path:
            try:
                # This is the default for csvw
                metadata = json.load(f"{data_path}-metadata.json")
            except:
                # If no metadata is available
                metadata = None
        else:
            metadata = Metadata.from_file(metadata_path)

        _name = name if name else metadata["title"]

        return cls(
            name=_name,
            df=df,
            metadata=metadata,
        )

    def select(self: _TableLayer, columns: List[str]) -> _TableLayer:
        info(f"Selected columns: {columns}.")
        return TableLayer(
            name=self.name,
            df=self.df.loc[:, columns],
            metadata=self.metadata
        )

    def where(self: _TableLayer, condition: Series) -> _TableLayer:
        frame = inspect.currentframe()
        code_input = inspect.getouterframes(frame, 1)
        code_input = [f for f in code_input if 'where(' in f.code_context[0]]
        code_input = code_input[0].code_context[0]
        code_input = code_input[6 + code_input.find('where('):code_input.rfind(')')]
        info(f"Selected rows where {code_input}.")
        return TableLayer(
            name=self.name,
            df=self.df.loc[condition, :],
            metadata=self.metadata
        )

    def join(self: _TableLayer,
             other: DataFrame,
             how: str = "left",
             kwargs: Dict[str, Any] = None
             ) -> _TableLayer:
        info(f"Joined to {other.info()}.")
        return TableLayer(
            name=self.name,
            df=self.df.merge(right=other, how=how, **kwargs),
            metadata=self.metadata
        )
