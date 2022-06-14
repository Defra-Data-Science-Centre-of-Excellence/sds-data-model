from dataclasses import dataclass
import io
import json
import requests
from typing import Any, Dict, Generator, List, Optional, TypeVar, Tuple, Union

from pandas import DataFrame, read_csv
from urllib.parse import urlparse

from sds_data_model.metadata import Metadata

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
        return TableLayer(
            name=self.name,
            df=self.df.loc[:, columns],
            metadata=self.metadata
            )

    # def where():

    # def join():
