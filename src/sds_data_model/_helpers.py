from json import load
from pathlib import Path
from typing import Optional

from sds_data_model.metadata import Metadata

def _read_metadata(data_path: str, metadata_path: str) -> Optional[Metadata]:
    """Read metadata from path, or json sidecar, or return None."""
    json_sidecar = Path(f"{data_path}-metadata.json")
    if not metadata_path and json_sidecar.exists():
        with open(json_sidecar, "r") as json_metadata:
            metadata = load(json_metadata)
    elif not metadata_path:
        metadata = None
    else:
        metadata = Metadata.from_file(metadata_path)
    return metadata