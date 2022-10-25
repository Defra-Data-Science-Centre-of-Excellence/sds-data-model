"""Private functions for the DataFrame wrapper class."""
from json import load
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from affine import Affine
from geopandas import GeoDataFrame, GeoSeries
from numpy import arange, zeros
from pandas import DataFrame as PandasDataFrame
from rasterio.features import geometry_mask
from xarray import DataArray

from sds_data_model.constants import (
    BNG_XMAX,
    BNG_XMIN,
    BNG_YMAX,
    BNG_YMIN,
    CELL_SIZE,
    OUT_SHAPE,
)
from sds_data_model.metadata import Metadata


def _get_name(
    metadata: Optional[Metadata] = None,
    name: Optional[str] = None,
) -> str:
    """Gets name provided.

    Returns the provided name, the associated metadata title,
     or raises an error.

    Examples:
        If `name` is provided, the function returns that name:
        >>> _get_name(
            name="ramsar",
        )
        'ramsar'

        If `name` isn't provided but a :class: Metadata object is,
        the function returns `metadata.title`:
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
            information parsed from GEMINI XML. Defaults to None.
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
        metadata_path (Optional[str], optional): Path to a `UK GEMINI`_ metadata file.
            Defaults to None.
        metadata_kwargs (Optional[Dict[str, Any]], optional): Key word arguments to
            be passed to the requests `get`_ method when reading xml metadata from
            a URL. Defaults to None.

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


def _to_zarr_region(
    pdf: PandasDataFrame,
    data_array_name: str,
    path: str,
    cell_size: int = CELL_SIZE,
    out_shape: Tuple[int, int] = OUT_SHAPE,
    bng_ymax: int = BNG_YMAX,
    geometry_column_name: str = "geometry",
    invert: bool = True,
    dtype: str = "uint8",
) -> PandasDataFrame:
    """Rasterises a BNG grid cell and writes to the relevant region of a `zarr` file.

    This function will be converted into a `pandas_udf`_ and called by `applyInPandas`_
    within the :meth:`DataFrameWrapper.to_zarr` method.

    Writing to a zarr file is a side effect of the method hence the input and the
    output is the same Pandas DataFrame.

    This function assumes that the Pandas DataFrame contains a column with BNG bounds
    that is named "bounds".

    Args:
        pdf (PandasDataFrame): A Pandas DataFrame.
        data_array_name (str): DataArray name given by the user.
        path (str): Path to save the zarr file including file name.
        cell_size (int): The resolution of the cells in the DataArray. Defaults to
            CELL_SIZE.
        out_shape (Tuple[int, int]): The shape (height, width) of the DataArray.
            Defaults to OUT_SHAPE.
        bng_ymax (int): The maximum y value of the British National Grid.
            Defaults to BNG_YMAX.
        geometry_column_name (str): The name of the geometry column in the
            Pandas DataFrame. Defaults to "geometry".
        invert (bool): If True, the centre point of pixels that fall inside
            a geometry will be turned-on. If False, they will be turned-off.
            Defaults to True.
        dtype (str): Data type of the DataArray. Defaults to "uint8".

    Returns:
        PandasDataFrame: The input Pandas DataFrame is returned unchanged.

    .. _`pandas_udf`:
        https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.functions.pandas_udf.html  # noqa: B950

    .. _`applyInPandas`:
        https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.GroupedData.applyInPandas.htm
    """    
    minx, miny, maxx, maxy = pdf["bounds"][0]

    transform = Affine(cell_size, 0, minx, 0, -cell_size, maxy)

    gpdf = GeoDataFrame(
            data=pdf,
            geometry=GeoSeries.from_wkb(pdf[geometry_column_name]),
            crs="EPSG:27700",
            )

    mask = geometry_mask(
        geometries=gpdf[geometry_column_name],
        out_shape=out_shape,
        transform=transform,
        invert=invert,
    ).astype(dtype)

    (
        DataArray(
            data=mask,
            coords={
                "northings": (
                    "northings",
                    arange(maxy - (cell_size / 2), miny, -cell_size),
                ),
                "eastings": (
                    "eastings",
                    arange(minx + (cell_size / 2), maxx, cell_size),
                ),
            },
            name=data_array_name,
        )
        .to_dataset()
        .to_zarr(
            store=path,
            mode="r+",
            region={
                "northings": slice(
                    int((maxy - bng_ymax) / -cell_size),
                    int((miny - bng_ymax) / -cell_size),
                ),
                "eastings": slice(int(minx / cell_size), int(maxx / cell_size)),
            },
        )
    )

    return pdf


def _create_dummy_dataset(
    data_array_name: str,
    path: str,
    dtype: str = "uint8",
    cell_size: int = CELL_SIZE,
    bng_xmin: int = BNG_XMIN,
    bng_xmax: int = BNG_XMAX,
    bng_ymin: int = BNG_YMIN,
    bng_ymax: int = BNG_YMAX,
) -> None:
    """An empty DataArray is created to the size of the BNG with co-ordinates. It is changed to a Dataset and stored temporarily.

    Examples:

        >>> d_dataset = _create_dummy_dataset(
                data_array_name="dummy", 
                path = "/dbfs/mnt/lab/unrestricted/piumi.algamagedona@defra.gov.uk/dummy"
            )

        >>> d_dataset
        Delayed('_finalize_store-31bc6052-52db-49e8-bc87-fc8f7c6801ed')

    Args:
        data_array_name (str): Name of the DataArray given
        path (str): Path to the zarr file with the name of the zarr file.
        dtype (str, optional): Data type of the geometry mask. Defaults to "uint8".
        cell_size (int, optional): Size of one raster cell in the DataArray. Defaults to CELL_SIZE.
        bng_xmin (int, optional): British National Grid minimum X axis value. Defaults to BNG_XMIN.
        bng_xmax (int, optional): British National Grid maximum X axis value. Defaults to BNG_XMAX.
        bng_ymin (int, optional): British National Grid minimum Y axis value. Defaults to BNG_YMIN.
        bng_ymax (int, optional): British National Grid maximum Y axis value. Defaults to BNG_YMAX.

    Returns:
        _type_: None. A dask delayed object is created.
    """

    return (
        DataArray(
            data=zeros(
                shape=(int(bng_ymax / cell_size), int(bng_xmax / cell_size)),
                dtype=dtype,
            ),
            coords={
                "northings": ("northings", arange(bng_ymax, bng_ymin, -cell_size)),
                "eastings": ("eastings", arange(bng_xmin, bng_xmax, cell_size)),
            },
            name=data_array_name,
        )
        .to_dataset()
        .to_zarr(
            store=path,
            mode="w",
            compute=False,
        )
    )
