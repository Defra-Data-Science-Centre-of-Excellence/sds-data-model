from itertools import product
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple, Union, cast

from affine import Affine
from cv2 import INTER_LINEAR, INTER_NEAREST, resize
from numpy import arange, array
from numpy.typing import NDArray
from rasterio.drivers import raster_driver_extensions
from rioxarray.rioxarray import affine_to_coords
from xarray import DataArray, Dataset, merge, open_dataset

from sds_data_model._dataframe import _create_dummy_dataset
from sds_data_model._vector import _check_layer_projection
from sds_data_model.constants import BNG_XMAX, BNG_XMIN, BNG_YMAX, CELL_SIZE


def _has_wrong_cell_size(
    transform: Affine,
    expected_cell_size: int,
) -> bool:
    """Return `True` if the cell size is not what is expected.

    Return `False` if either the x (a) and y (e) values of the input transform do
    **not equal** the expected cell size.

    Args:
        transform (Affine): Input transform, the data being checked.
        expected_cell_size (int): The expected cell size in the transform.

    Returns:
        bool: An indication in :func:`_reshape_raster` whether to resample data.
    """
    if transform.a != expected_cell_size or -transform.e != expected_cell_size:
        return True
    else:
        return False


def _has_wrong_shape(
    transform: Affine,
    expected_cell_size: int,
    expected_x_min: int,
    expected_y_max: int,
) -> bool:
    """Return true if the shape/extent is **not equal** to the expected shape.

    Args:
        transform (Affine): Input transform, the data being checked.
        expected_cell_size: The expected cell size.
        expected_x_min (int): The expected value for the x minimum.
        expected_y_max (int): The expected value for the y maximum.

    Returns:
        bool: An indication in :func:`_reshape_raster` whether to reshape data.
    """
    if (transform.f / -transform.e) != (expected_y_max / expected_cell_size) or (
        transform.c / transform.a
    ) != (expected_x_min / expected_cell_size):
        return True
    else:
        return False


def _get_resample_shape(
    shape: Tuple[int, int],
    transform: Affine,
    expected_cell_size: int,
) -> NDArray:
    """Determine the scaled shape from an input cell size.

    Args:
        shape (Tuple[int, int]): Input shape.
        transform (Affine): Shape transform. Used to determine current cell size.
        expected_cell_size (int): New cell size for data. Acts as the resampling factor.

    Returns:
        NDArray: 2d array of (height, width).
    """
    return (
        ((array([transform.a, -transform.e]) / expected_cell_size) * shape)
        .round()
        .astype("int")
    )


def _resample_cell_size(
    _array: NDArray,
    resample_shape: Union[Sequence, NDArray],
    expected_cell_size: int,
    expected_x_max: int,
    expected_x_min: int,
    expected_y_max: int,
    expected_y_min: int,
    categorical: bool,
    name: str,
) -> Dataset:
    """Returns a `Dataset` with scaled/resampled data using `OpenCV`_.

    Args:
        _array (NDArray): Input data.
        resample_shape (Union[Sequence, NDArray]): Shape to resample to.
        categorical (bool): determines which resampling/interpolation is used.
            Set to False will use bilinear. Set to True will use nearest-neighbor.
        expected_cell_size (int): cell size of resampled array.
        expected_x_max (int): x maximum.
        expected_x_min (int): x minimum.
        expected_y_max (int): y maximum.
        expected_y_min (int): y minimum.
        name (str): `DataArray` name.

    Returns:
        Dataset: A dataset with resampled data.

    .. _`OpenCV`:
        https://github.com/opencv/opencv-python
    """
    return DataArray(
        data=resize(
            src=_array,
            dsize=resample_shape[::-1],
            interpolation=INTER_NEAREST if categorical else INTER_LINEAR,
        ),
        coords={
            "northings": arange(
                expected_y_max - (expected_cell_size / 2),
                expected_y_min,
                -expected_cell_size,
            ),
            "eastings": arange(
                expected_x_min + (expected_cell_size / 2),
                expected_x_max,
                expected_cell_size,
            ),
        },
        dims=("northings", "eastings"),
        name=name,
    ).to_dataset()


def _check_no_data(
    data_array: DataArray,
    nodata: Optional[float],
) -> Optional[float]:
    """Checks and conditionally writes a `nodata` value to data_array.

    Args:
        data_array (DataArray): Input `DataArray`.
        nodata (Optional[float]): Value that will be written to the
            input `DataArray` if it is not `None`.

    Raises:
        ValueError: If the `nodata` arg is `None` and the input `DataArray`
            does not have a nodata value (that is not equal to None).

    Returns:
        Optional[float]: `nodata§ if `nodata` provided.
    """
    if nodata is not None:
        data_array.rio.write_nodata(nodata, inplace=True)
        return nodata
    elif data_array.rio.nodata is None:
        raise ValueError(
            "Input dataset does not have a nodata value. One must be provided."
        )
    return None


def _select_bands(
    dataset: Dataset,
    bands: Optional[Union[List[int], List[str]]],
    engine: Optional[str],
) -> List[DataArray]:
    """Extracts each band from the input dataset.

    Specific bands are selected using the `bands` arg if it is provided.
    If the `engine` arg is rasterio, bands may have a `long_name` attr.
    If this attr doesn't exist the position/index of the band is used as its name.

    Args:
        dataset (Dataset): Input dataset.
        bands (Optional[Union[List[int], List[str]]]):
            List of band numbers or names.
        engine (Optional[str]): Engine used by open_dataset.

    Returns:
        List[DataArray]: List of `DataArray`s (bands).
    """
    if engine == "rasterio":
        try:
            band_names = dataset.band_data.long_name
        except AttributeError:
            band_names = range(1, len(dataset.band_data) + 1)
        data_arrays = [
            data_array.rename(f"{band_name}").drop("band")
            for band_name, data_array in zip(band_names, dataset.band_data)
        ]
    else:
        data_arrays = [dataset[array_name] for array_name in dataset.rio.vars]

    if bands:
        data_arrays = [
            data_array
            for data_array in data_arrays
            if data_array.name in map(str, bands)
        ]
    return data_arrays


def _reshape_raster(
    dataset: Dataset,
    expected_cell_size: int,
    expected_x_max: int,
    expected_x_min: int,
    expected_y_max: int,
    bands: Optional[Union[List[int], List[str]]],
    categorical: Union[bool, Dict[Union[int, str], bool]],
    out_path: Optional[str],
    nodata: Optional[float],
    engine: Optional[str],
) -> Dataset:
    """Condtionally reshape (to arg vals) and return input dataset.

    If a `DataArray` of the input dataset does not conform to the expected extent
    an empty zarr will be written to disk in that shape.
    If the cell size of the `DataArray` isn't equal to the expected,
    it will be resampled. The `DataArray` is then written to the empty zarr.
    This process is executed in chunks as significant upsampling can cause
    memory issues.
    If the `Dataset`'s contents does conform to the expected extent and cell size
    it is returned.

    Args:
        dataset (Dataset): Input `Dataset` with **chunked** `DataArray`(s).
        expected_cell_size (int): expected cell size.
        expected_x_max (int): expected x maximum.
        expected_x_min (int): expected x minimum.
        expected_y_max (int): expected y maximum.
        bands (Optional[Union[List[int], List[str]]]): List of bands to select
            from the `Dataset`.
        categorical (Union[bool, Dict[Union[int, str], bool]]): `bool` or `dict`
            mapping ({band : bool}) of the interpolation used to resample.
        out_path (Optional[str]): Path to write reshaped data.
        nodata (Optional[float]): Value that will fill the grid where
            there is no data (if it is not `None`).
        engine (Optional[str]): Engine used by open_dataset.

    Returns:
        Dataset: A dataset of `DataArray`s that conform to the `expected_...` values.
    """
    transform = dataset.rio.transform()
    data_arrays = _select_bands(dataset, bands=bands, engine=engine)
    dims = ("eastings", "northings")
    if _has_wrong_shape(
        transform=transform,
        expected_cell_size=expected_cell_size,
        expected_x_min=expected_x_min,
        expected_y_max=expected_y_max,
    ):
        _dtype, _nodata = {}, {}
        for data_array in data_arrays:
            _dtype[str(data_array.name)] = data_array.dtype.name
            _nodata[str(data_array.name)] = (
                data_array.nodata
                if "nodata" in data_array.attrs
                else _check_no_data(data_array, nodata)
            )
        _create_dummy_dataset(
            path=cast(str, out_path),
            dtype=_dtype,
            nodata=_nodata,
            cell_size=expected_cell_size,
            bng_xmin=expected_x_min,
            bng_xmax=expected_x_max,
            bng_ymax=expected_y_max,
            columns=[data_array.name for data_array in data_arrays],
            mask_name=None,
            lookup=None,
            metadata=None,
        )

        if _has_wrong_cell_size(transform, expected_cell_size):
            categorical = (
                {str(data_array.name): categorical for data_array in data_arrays}
                if isinstance(categorical, bool)
                else {str(_band): _bool for _band, _bool in categorical.items()}
            )

        for data_array in data_arrays:
            for indices in product(*map(range, data_array.data.blocks.shape)):
                chunk_shape = (
                    _get_resample_shape(
                        data_array.data.blocks[indices].shape,
                        transform,
                        expected_cell_size,
                    )
                    if _has_wrong_cell_size(transform, expected_cell_size)
                    else array(data_array.data.blocks[indices].shape)
                )
                grid_chunk_shape = chunk_shape * expected_cell_size
                chunks = cast(Tuple[Tuple[int, ...], ...], data_array.chunks)
                y_delta, x_delta = sum(chunks[0][: indices[0]]), sum(
                    chunks[1][: indices[1]]
                )

                if _has_wrong_cell_size(transform, expected_cell_size):
                    y_delta, x_delta = _get_resample_shape(
                        (y_delta, x_delta), transform, 1
                    )
                else:
                    y_delta *= expected_cell_size
                    x_delta *= expected_cell_size
                chunk_maxy, chunk_minx = transform.f - y_delta, transform.c + x_delta
                chunk_miny, chunk_maxx = (
                    chunk_maxy - grid_chunk_shape[0],
                    chunk_minx + grid_chunk_shape[1],
                )

                _resample_cell_size(
                    data_array.data.blocks[indices].compute(),
                    resample_shape=chunk_shape,
                    expected_cell_size=expected_cell_size,
                    expected_x_max=chunk_maxx,
                    expected_x_min=chunk_minx,
                    expected_y_max=chunk_maxy,
                    expected_y_min=chunk_miny,
                    categorical=categorical[str(data_array.name)]
                    if isinstance(categorical, dict)
                    else False,
                    name=str(data_array.name),
                ).drop_vars([*dims]).to_zarr(
                    store=out_path,
                    mode="r+",
                    region={
                        dims[1]: slice(
                            int(
                                round(
                                    (chunk_maxy - expected_y_max) / -expected_cell_size
                                )
                            ),
                            int(
                                round(
                                    (chunk_miny - expected_y_max) / -expected_cell_size
                                )
                            ),
                        ),
                        dims[0]: slice(
                            int(round(chunk_minx / expected_cell_size)),
                            int(round(chunk_maxx / expected_cell_size)),
                        ),
                    },
                )
        _dataset = open_dataset(
            cast(str, out_path),
            engine="zarr",
            decode_coords="all",
            mask_and_scale=False,
        )
    else:
        _dataset = merge(
            data_array.to_dataset(name=data_array.name) for data_array in data_arrays
        )
        if set(_dataset.dims) != set(dims):
            _dataset = _dataset.rename(
                {dataset.rio.x_dim: dims[0], dataset.rio.y_dim: dims[1]}
            )
        _dataset.update(
            affine_to_coords(
                Affine(
                    expected_cell_size,
                    0,
                    expected_x_min,
                    0,
                    -expected_cell_size,
                    expected_y_max,
                ),
                height=int(expected_y_max / expected_cell_size),
                width=int(expected_x_max / expected_cell_size),
                y_dim=dims[1],
                x_dim=dims[0],
            ),
        )
    _dataset.rio.set_spatial_dims(*dims)
    return _dataset


def _read_dataset_from_file(
    data_path: str,
    bands: Optional[Union[List[int], List[str]]] = None,
    categorical: Union[bool, Dict[Union[int, str], bool]] = False,
    nodata: Optional[float] = None,
    engine: Optional[str] = None,
    decode_coords: Optional[Union[bool, Literal["coordinates", "all"]]] = "all",
    out_path: Optional[str] = None,
    chunks: Optional[Union[int, Dict[Any, Any], Literal["auto"]]] = "auto",
    expected_cell_size: int = CELL_SIZE,
    expected_x_min: int = BNG_XMIN,
    expected_x_max: int = BNG_XMAX,
    expected_y_max: int = BNG_YMAX,
) -> Dataset:
    """Returns a `Dataset` from an input raster according to the constant value args.

    Opens the raster from the input filename using `open_dataset`. CRS is checked.
    Data and args are passed to `_reshape_raster` that will conditionally
    reshape, write and read data.

    Args:
        data_path (str): File path to raster.
        bands (Optional[Union[List[int], List[str]]], optional): List of bands to select
            from the `Dataset`. Defaults to None.
        categorical (Union[bool, Dict[Union[int, str], bool]], optional):
            `bool` or `dict` mapping ({band : bool}) of the interpolation used
            to resample. Defaults to False.
        nodata (Optional[float], optional): Value that will fill the grid where
            there is no data (if it is not `None`). Defaults to None.
        engine (Optional[str], optional): Engine used by `open_dataset`.
            Defaults to None.
        decode_coords (Optional[Union[bool, Literal["coordinates", "all"]]], optional):
            Value used by `open_dataset`. Variable upon `engine` selection.
            Defaults to "all".
        out_path (Optional[str]): Path to write reshaped data. Defaults to None.
        chunks (Optional[Union[int, Dict[Any, Any], Literal["auto"]]]): Chunk size or
            state. Passed to `chunks` in `xarray.open_dataset`. Defaults to "auto".
        expected_cell_size (int): Cell size to resample the data to if it is not already
            this value. Defaults to CELL_SIZE.
        expected_x_min (int): x minimum. Defaults to BNG_XMIN.
        expected_x_max (int): x maximum (width). Defaults to BNG_XMAX.
        expected_y_max (int): y maximum (height). Defaults to BNG_YMAX.

    Returns:
        Dataset: A dataset of `DataArray`s that conform to the `expected_...` values.
    """
    suffix = Path(data_path).suffixes[0]

    if engine:
        _engine = engine
    elif suffix == ".zarr":
        _engine = "zarr"
        _decode_coords = decode_coords
    elif suffix[1:] in raster_driver_extensions().keys():
        _engine = "rasterio"
        _decode_coords = None

    dataset = open_dataset(
        data_path,
        engine=_engine,
        decode_coords=_decode_coords,
        mask_and_scale=False,
        chunks=chunks,
    )

    _check_layer_projection({"crs": dataset.rio.crs.to_string()})

    return _reshape_raster(
        dataset=dataset,
        bands=bands,
        categorical=categorical,
        out_path=out_path,
        nodata=nodata,
        engine=_engine,
        expected_cell_size=expected_cell_size,
        expected_x_min=expected_x_min,
        expected_x_max=expected_x_max,
        expected_y_max=expected_y_max,
    )
