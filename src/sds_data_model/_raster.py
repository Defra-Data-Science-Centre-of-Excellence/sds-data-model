from pathlib import Path
from typing import Dict, List, Literal, Optional, Union

from affine import Affine
from cv2 import INTER_LINEAR, INTER_NEAREST, resize
from numpy import array, full
from numpy.typing import NDArray
from rasterio.drivers import raster_driver_extensions
from rioxarray.rioxarray import affine_to_coords
from xarray import DataArray, Dataset, merge, open_dataset

from sds_data_model._vector import _check_layer_projection
from sds_data_model.constants import BNG_XMAX, BNG_XMIN, BNG_YMAX, CELL_SIZE


def _has_wrong_cell_size(
    transform: Affine,
    expected_cell_size: int,
) -> bool:
    """Return true if the cell size of x (a) and y (e) in the \
    input transform are **not equal** to the expected cell size.

    Args:
        transform (Affine): Input transform, the data being checked.
        expected_cell_size (int): The expected cell size in the transform.

    Returns:
        bool: An indication in `_resample_and_reshape whether` to resample data.
    """
    if transform.a != expected_cell_size or -transform.e != expected_cell_size:
        return True
    else:
        return False


def _has_wrong_shape(
    transform: Affine,
    expected_x_min: int,
    expected_y_max: int,
) -> bool:
    """Return true if the shape/extent of the input data \
    is **not equal** to the expected shape.

    Args:
        transform (Affine): Input transform, the data being checked.
        expected_x_min (int): The expected value for the x minimum.
        expected_y_max (int): The expected value for the y maximum.

    Returns:
        bool: An indication in `_resample_and_reshape` whether to reshape/regrid data.
    """
    if transform.f != expected_y_max or transform.c != expected_x_min:
        return True
    else:
        return False


def _create_data_array(
    data_array: DataArray,
    data: NDArray,
    geotransform: str,
) -> DataArray:
    """An intermediate `DataArray` constructor for \
    the updating of shape/extent/transform.

    Args:
        data_array (DataArray): Original `DataArray`.
        data (NDArray): New array data.
        geotransform (str): New (gdal) geotransform.

    Returns:
        DataArray: `DataArray` with the input data and geotransform added.
    """
    return DataArray(
        data=data,
        coords={
            data_array.rio.grid_mapping: data_array[
                data_array.rio.grid_mapping
            ].rio.update_attrs({"GeoTransform": geotransform}),
        },
        dims=("northings", "eastings"),
        attrs={"nodata": data_array.rio.nodata},
    ).rename(data_array.name)


def _get_resample_shape(
    data_array: DataArray,
    expected_cell_size: int,
) -> NDArray:
    """Determine the new (scaled) shape of an input DataArray from a cell_size value.

    Args:
        data_array (DataArray): Input `DataArray`.
        expected_cell_size (int): New cell size for data. Acts as the resampling factor.

    Returns:
        NDArray: 2d array of (height, width).
    """
    return (
        (
            (
                array([data_array.rio.transform().a, -data_array.rio.transform().e])
                / expected_cell_size
            )
            * data_array.shape
        )
        .round()
        .astype("int")
    )


def _resample_cell_size(
    data_array: DataArray,
    expected_cell_size: int,
    categorical: bool,
) -> DataArray:
    """Returns a `DataArray` with scaled/resampled data using OpenCV.

    Args:
        data_array (DataArray): Input `DataArray`.
        expected_cell_size (int): New cell size for data. Acts as the resampling factor.
        categorical (bool): Determines which resampling/interpolation is used.
            Set to False will use bilinear. Set to True will use nearest-neighbour.

    Returns:
        DataArray
    """
    if categorical:
        interpolation = INTER_NEAREST
    else:
        interpolation = INTER_LINEAR
    resampled = resize(
        src=data_array.data,
        dsize=_get_resample_shape(data_array, expected_cell_size)[::-1],
        interpolation=interpolation,
    )

    return _create_data_array(
        data_array=data_array,
        data=resampled,
        geotransform=(
            f"{data_array.rio.transform().c} {expected_cell_size} 0 "
            f"{data_array.rio.transform().f} 0 -{expected_cell_size}"
        ),
    )


def _check_no_data(
    data_array: DataArray,
    nodata: Optional[float],
) -> None:
    """Checks and conditionally writes a `nodata` value to data_array.

    Args:
        data_array (DataArray): Input `DataArray`.
        nodata (Optional[float]): Value that will be written to the
            input `DataArray` if it is not `None`.

    Raises:
        Exception: If the `nodata` arg is `None` and the input `DataArray`
            does not have a nodata value (that is not equal to None).
    """
    if nodata is not None:
        data_array.rio.write_nodata(nodata, inplace=True)
    elif data_array.rio.nodata is None:
        raise Exception(
            "Input dataset does not have a nodata value. One must be provided."
        )


def _get_origin_offset(
    data_array: DataArray,
    expected_x_min: int,
    expected_y_max: int,
) -> NDArray:
    """Determines the offset of the input `DataArray` from the origin (ymax, xmin).

    Args:
        data_array (DataArray): Input `DataArray`.
        expected_x_min (int): x minimum.
        expected_y_max (int): y maximum.

    Returns:
        NDArray: 2d array of the offset, (height, width).
    """
    return (
        (
            array(
                [
                    expected_y_max - data_array.rio.transform().f,
                    expected_x_min + data_array.rio.transform().c,
                ]
            )
            / int(data_array.rio.transform().a)
        )
        .round()
        .astype("int")
    )


def _to_grid_extent(
    data_array: DataArray,
    expected_x_min: int,
    expected_x_max: int,
    expected_y_max: int,
    nodata: Optional[float],
) -> DataArray:
    """Returns a `DataArray` where the data of the input (`data_array`) has been \
    inserted into a grid of shape (`expected_y_max`, `expected_x_max`).

    Where the data of the input `DataArray` does not occupy the grid, these
    cells are set to the `nodata` value if it is provided, else the
    value of the `nodata` attribute of the input `DataArray`.

    Args:
        data_array (DataArray): Input `DataArray`.
        expected_x_min (int): x minimum.
        expected_x_max (int): x maximum (width).
        expected_y_max (int): y maxium (height).
        nodata (Optional[float]): value that will fill the grid where
            there is no data (if it is not `None`).

    Returns:
        DataArray: A `DataArray` the shape of the specified grid,
        containing the data of the input `DataArray`.
    """
    _check_no_data(data_array, nodata)

    offset = _get_origin_offset(data_array, expected_x_min, expected_y_max)

    bng = full(
        shape=(
            array([expected_y_max, expected_x_max]) / data_array.rio.transform().a
        ).astype("int"),
        fill_value=data_array.rio.nodata,
        dtype=data_array.dtype,
    )

    bng[
        offset[0] : offset[0] + data_array.shape[0],
        offset[1] : offset[1] + data_array.shape[1],
    ] = data_array.data

    return _create_data_array(
        data_array=data_array,
        data=bng,
        geotransform=(
            f"{expected_x_min} {data_array.rio.transform().a} 0 "
            f"{expected_y_max} 0 {data_array.rio.transform().e}"
        ),
    )


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


def _resample_and_reshape(
    dataset: Dataset,
    expected_cell_size: int,
    expected_x_min: int,
    expected_x_max: int,
    expected_y_max: int,
    bands: Optional[Union[List[int], List[str]]],
    categorical: Union[bool, Dict[Union[int, str], bool]],
    nodata: Optional[float],
    engine: Optional[str],
) -> Dataset:
    """Checks and conditionally changes the cell size and shape/extent \
    of the input `Dataset` against the input values.

    Functions will resample every DataArray in a `Dataset`
    (to `expected_cell_size`) and reshape/regrid (to `expected_y_max`, `expected_x_max`)
    if the respective checks return `True`. The dataset is then reconstructed
    (arrays merged into dataset, dimensions renamed if they aren't named
    "northings" and "eastings" and addition of coordinate variables).

    Args:
        dataset (Dataset): Input dataset.
        expected_cell_size (int): Cell size to resample the
            data to if it is not already this value.
        expected_x_min (int): x minimum.
        expected_x_max (int): x maximum (width).
        expected_y_max (int): y maximum (height).
        bands (Optional[Union[List[int], List[str]]]): List of bands to select
            from the `Dataset`.
        categorical (Union[bool, Dict[Union[int, str], bool]]): `bool` or `dict`
            mapping ({band : bool}) of the interpolation used to resample.
            False will use bilinear. True will use nearest-neighbour.
        nodata (Optional[float]): Value that will fill the grid where
            there is no data (if it is not `None`).
        engine (Optional[str]): Engine used by open_dataset.

    Returns:
        Dataset: A dataset of `DataArray`s that conform to the `expected_...` values.
    """
    transform = dataset.rio.transform()
    data_arrays = _select_bands(dataset, bands=bands, engine=engine)
    if _has_wrong_cell_size(
        transform=transform,
        expected_cell_size=expected_cell_size,
    ):
        if isinstance(categorical, bool):
            categorical = {
                str(data_array.name): categorical for data_array in data_arrays
            }
        else:
            categorical = {str(_band): _bool for _band, _bool in categorical.items()}
        for index, data_array in enumerate(data_arrays):
            data_arrays[index] = _resample_cell_size(
                data_array=data_array,
                expected_cell_size=expected_cell_size,
                categorical=categorical[str(data_array.name)],
            )

    if _has_wrong_shape(
        transform=transform,
        expected_x_min=expected_x_min,
        expected_y_max=expected_y_max,
    ):
        for index, data_array in enumerate(data_arrays):
            data_arrays[index] = _to_grid_extent(
                data_array=data_array,
                expected_x_min=expected_x_min,
                expected_x_max=expected_x_max,
                expected_y_max=expected_y_max,
                nodata=nodata,
            )

    _dataset = merge(
        data_array.to_dataset(name=data_array.name) for data_array in data_arrays
    )
    dims = ("eastings", "northings")
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
    expected_cell_size: int = CELL_SIZE,
    expected_x_min: int = BNG_XMIN,
    expected_x_max: int = BNG_XMAX,
    expected_y_max: int = BNG_YMAX,
) -> Dataset:
    """Returns a `Dataset` from an input raster according to the constant value args.

    Opens the raster from the input filename using `open_dataset`. CRS is checked.
    Data and args are passed to `_resample_and_reshape` that will conditionally
    resample and reshape the input data.

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
        expected_cell_size (int): Cell size to resample the data to if it is not already
            this value. Defaults to CELL_SIZE.
        expected_x_min (int): x minimum. Defaults to BNG_XMIN.
        expected_x_max (int): x maximum (width). Defaults to BNG_XMAX.
        expected_y_max (int): y maxium (height). Defaults to BNG_YMAX.

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
    )

    _check_layer_projection({"crs": dataset.rio.crs.to_string()})

    return _resample_and_reshape(
        dataset=dataset,
        bands=bands,
        categorical=categorical,
        nodata=nodata,
        engine=_engine,
        expected_cell_size=expected_cell_size,
        expected_x_min=expected_x_min,
        expected_x_max=expected_x_max,
        expected_y_max=expected_y_max,
    )
