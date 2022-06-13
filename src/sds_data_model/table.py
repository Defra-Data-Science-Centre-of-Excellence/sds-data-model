from dataclasses import dataclass
import json
from typing import Any, Dict, Generator, List, Optional, TypeVar, Tuple, Union

from pandas import DataFrame, read_csv

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
        df = read_csv(data_path, **data_kwargs)

        if not metadata_path:
            # This is the default for csvw
            metadata = json.load(f"{data_path}-metadata.json")
        else:
            metadata = Metadata.from_file(metadata_path)

        _name = name if name else metadata["title"]

        return cls(
            name=_name,
            df=df,
            metadata=metadata,
        )
