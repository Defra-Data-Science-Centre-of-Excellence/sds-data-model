from json import load
from pathlib import Path
from typing import Optional, Dict, Any

from sds_data_model.metadata import Metadata


def _get_name(
    metadata: Optional[Metadata] = None,
    name: Optional[str] = None,
) -> str:
    """Returns the provided name, the associated metadata title,
     or raises an error.

    Examples:
        If `name` is provided, the function returns that name:
        >>> _get_name(
            name="ramsar",
        )
        'ramsar'
        If `name` isn't provided but a :class: Metadata object is,
        the function returns
        `metadata.title`:
        >>> metadata = _get_metadata(
            data_path="tests/test_metadata/ramsar.gpkg",
            metadata_path="tests/test_metadata/ramsar.xml",
        )
        >>> _get_name(
            metadata=metadata,
        )
        'Ramsar (England)'
        If both are provided, `name` is preferred:
        >>> metadata = _get_metadata(
            data_path="tests/test_metadata/ramsar.gpkg",
            metadata_path="tests/test_metadata/ramsar.xml",
        )
        >>> _get_name(
            name="ramsar",
            metadata=metadata,
        )
        'ramsar'
        If neither are provided, an error is raised:
        >>> _get_name()
        ValueError: If there isn't any metadata, a name must be supplied.

    Args:
        metadata (Optional[Metadata]): A :class: Metadata object containing 
        imformation parsed from GEMINI XML. Defaults to None.
        name (Optional[str]): A name, provided by the caller. Defaults to None.
    Raises:
        ValueError: If neither a name nor a `Metadata` are provided.
    Returns:
        str: A name for the dataset.
    """
    if name:
        return name
    elif metadata:
        return metadata.title
    else:
        raise ValueError("If there isn't any metadata, a name must be supplied.")


def _get_metadata(
    data_path: str,
    metadata_path: Optional[str] = None,
    metadata_kwargs: Optional[Dict[str, Any]] = None,
) -> Optional[Metadata]:
    """Read metadata from path, or json sidecar, or return None.
    Examples:
        If `metadata_path` is provided, the function will read that:
        >>> metadata = _get_metadata(
            data_path="tests/test_metadata/ramsar.gpkg",
            metadata_path="tests/test_metadata/ramsar.xml",
        )
        >>> metadata.title
        'Ramsar (England)'
        If `metadata_path` isn't provided but a json `sidecar`_ file exists,
        the function will read that:
        >>> from os import listdir
        >>> listdir("tests/test_metadata")
        ['ramsar.gpkg', 'ramsar.gpkg-metadata.json']
        >>> metadata = _get_metadata(
            data_path="tests/test_metadata/ramsar.gpkg",
        )
        >>> metadata.title
        'Ramsar (England)'
        If `metadata_path` isn't provided and there isn't a json sidecar file,
        the function will return `None`:
        >>> from os import listdir
        >>> listdir("tests/test_metadata")
        ['ramsar.gpkg']
        >>> metadata = _get_metadata(
            data_path="tests/test_metadata/ramsar.gpkg",
        )
        >>> metadata is None
        True
    Args:
        data_path (str): Path to the vector file.
        metadata_path (Optional[str]): Path to a `UK GEMINI`_ metadata file.
            Defaults to None.
        metadata_kwargs (Optional[Dict[str, Any]]): Key word arguments to be passed to
            the requests `get`_ method when reading xml metadata from a URL. Defaults
            to None.

    Returns:
        Optional[Metadata]: An instance of :class: Metadata
    .. _`UK GEMINI`:
        https://www.agi.org.uk/uk-gemini/
    .. _`sidecar`:
        https://en.wikipedia.org/wiki/Sidecar_file
    """
    json_sidecar = Path(f"{data_path}-metadata.json")
    if metadata_path:
        metadata = Metadata.from_file(metadata_path, metadata_kwargs)
    elif not metadata_path and json_sidecar.exists():
        with open(json_sidecar, "r") as json_metadata:
            metadata_dictionary = load(json_metadata)
            metadata = Metadata(**metadata_dictionary)
    else:
        metadata = None
    return metadata
