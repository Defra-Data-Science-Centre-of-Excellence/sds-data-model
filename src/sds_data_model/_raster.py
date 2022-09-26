from typing import List, Optional, Sequence, Union

from affine import Affine
from cv2 import INTER_LINEAR, INTER_NEAREST, resize
from numpy import array, fromstring, full
from numpy.typing import NDArray
from rioxarray.rioxarray import affine_to_coords
from xarray import DataArray, Dataset, merge

from sds_data_model.constants import BNG_XMAX, BNG_XMIN, BNG_YMAX, CELL_SIZE


def _has_wrong_cell_size(
    transform: Affine,
    expected_cell_size: int,
) -> bool:
    if transform.a != expected_cell_size or -transform.e != expected_cell_size:
        return True
    else:
        return False


def _has_wrong_shape(
    transform: Affine,
    expected_x_min: int,
    expected_y_max: int,
) -> bool:
    if transform.f != expected_y_max or transform.c != expected_x_min:
        return True
    else:
        return False


def _create_data_array(
    data_array: DataArray,
    data: NDArray,
    geotransform: str,
) -> DataArray:
    _data_array = DataArray(
        data=data,
        coords={
            **affine_to_coords(
                Affine.from_gdal(*fromstring(geotransform, sep=" ").tolist()),
                height=data.shape[0],
                width=data.shape[1],
                y_dim="northings",
                x_dim="eastings",
            ),
            data_array.rio.grid_mapping: data_array[
                data_array.rio.grid_mapping
            ].rio.update_attrs({"GeoTransform": geotransform}),
        },
        dims=("northings", "eastings"),
        attrs={"nodata": data_array.rio.nodata},
    ).rename(data_array.name)
    _data_array.rio.set_spatial_dims("eastings", "northings")
    return _data_array


def _get_resample_shape(
    data_array: DataArray,
) -> NDArray:
    return (
        (
            (
                array([data_array.rio.transform().a, -data_array.rio.transform().e])
                / CELL_SIZE
            )
            * data_array.shape
        )
        .round()
        .astype("int")
    )


def _resample_cellsize(
    data_array: DataArray,
    categorical: bool = False,
) -> DataArray:
    if categorical:
        interpolation = INTER_NEAREST
    else:
        interpolation = INTER_LINEAR
    resampled = resize(
        src=data_array.data,
        dsize=_get_resample_shape(data_array)[::-1],
        interpolation=interpolation,
    )

    return _create_data_array(
        data_array=data_array,
        data=resampled,
        geotransform=(
            f"{data_array.rio.transform().c} {CELL_SIZE} 0 "
            f"{data_array.rio.transform().f} 0 -{CELL_SIZE}"
        ),
    )


def _check_no_data(
    data_array: DataArray,
    nodata: Optional[float] = None,
) -> None:
    if nodata is not None:
        data_array.rio.write_nodata(nodata, inplace=True)
    elif data_array.rio.nodata is None:
        raise Exception(
            "Input dataset does not have a nodata value. One must be provided."
        )


def _get_bng_offset(
    data_array: DataArray,
) -> NDArray:
    return (
        (
            array(
                [BNG_YMAX - data_array.rio.transform().f, data_array.rio.transform().c]
            )
            / CELL_SIZE
        )
        .round()
        .astype("int")
    )


def _to_bng_extent(
    data_array: DataArray,
    nodata: Optional[float] = None,
) -> DataArray:
    _check_no_data(data_array, nodata)

    offset = _get_bng_offset(data_array)

    bng = full(
        shape=(array([BNG_YMAX, BNG_XMAX]) / CELL_SIZE).astype("int"),
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
        geotransform=f"{BNG_XMIN} {CELL_SIZE} 0 {BNG_YMAX} 0 -{CELL_SIZE}",
    )


def _select_data_arrays(
    dataset: Dataset,
    bands: Optional[Union[List[int], List[str]]] = None,
) -> List[DataArray]:
    try:
        data_arrays = [
            data_array.rename(f"{index + 1}").drop("band")
            for index, data_array in enumerate(dataset.band_data)
        ]
    except AttributeError:
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
    expected_y_max: int,
    bands: Optional[Union[List[int], List[str]]] = None,
    categorical: Union[bool, Sequence[bool]] = False,
    nodata: Optional[float] = None,
) -> Dataset:
    transform = dataset.rio.transform()
    data_arrays = _select_data_arrays(dataset, bands=bands)
    if _has_wrong_cell_size(
        transform=transform,
        expected_cell_size=expected_cell_size,
    ):
        if isinstance(categorical, bool):
            categorical = [categorical] * len(data_arrays)
        elif bands:
            categorical = [_bool for _, _bool in sorted(zip(bands, categorical))]
        for index, data_array in enumerate(data_arrays):
            data_arrays[index] = _resample_cellsize(
                data_array=data_array,
                categorical=categorical[index],
            )
    if _has_wrong_shape(
        transform=transform,
        expected_x_min=expected_x_min,
        expected_y_max=expected_y_max,
    ):
        for index, data_array in enumerate(data_arrays):
            data_arrays[index] = _to_bng_extent(
                data_array=data_array,
                nodata=nodata,
            )
    return merge(
        data_array.to_dataset(name=data_array.name) for data_array in data_arrays
    )
