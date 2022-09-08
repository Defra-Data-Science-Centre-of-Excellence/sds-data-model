from typing import List, Optional, Sequence, Union

from affine import Affine
from cv2 import INTER_LINEAR, INTER_NEAREST, resize
from numpy import array, full
from numpy.typing import NDArray
from xarray import DataArray, Dataset, merge

from sds_data_model.constants import BNG_XMAX, BNG_XMIN, BNG_YMAX, CELL_SIZE


def _check_cellsize(
    transform: Affine,
) -> Optional[bool]:
    if transform.a != CELL_SIZE or -transform.e != CELL_SIZE:
        return True
    else:
        return None


def _check_shape_and_extent(
    data_array: DataArray,
) -> Optional[bool]:
    if (
        data_array.shape != (BNG_YMAX / CELL_SIZE, BNG_XMAX / CELL_SIZE)
        or data_array.rio.transform().f != BNG_YMAX
        or data_array.rio.transform().c != BNG_XMIN
    ):
        return True
    else:
        return None


def _create_data_array(
    data_array: DataArray,
    data: NDArray,
    geotransform: str,
) -> DataArray:
    return DataArray(
        data=data,
        coords={
            data_array.rio.grid_mapping: data_array[
                data_array.rio.grid_mapping
            ].rio.update_attrs({"GeoTransform": geotransform})
        },
        dims=tuple(data_array.dims),
        attrs={"nodata": data_array.rio.nodata},
    ).rename(data_array.name)


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
    band: Union[List[int], List[str], None] = None,
) -> List[DataArray]:
    try:
        data_arrays = [
            data_array.rename(f"{index + 1}").drop("band")
            for index, data_array in enumerate(dataset.band_data)
        ]
    except AttributeError:
        data_arrays = [dataset[array_name] for array_name in dataset.rio.vars]

    if band:
        data_arrays = [
            data_array
            for data_array in data_arrays
            if data_array.name in map(str, band)
        ]
    return data_arrays


def _resample_and_reshape(
    dataset: Dataset,
    band: Union[List[int], List[str], None] = None,
    categorical: Union[bool, Sequence[bool]] = False,
    nodata: Optional[float] = None,
) -> Dataset:
    data_arrays = _select_data_arrays(dataset, band=band)
    if _check_cellsize(dataset.rio.transform()):
        if isinstance(categorical, bool):
            categorical = [categorical] * len(data_arrays)
        elif band:
            categorical = [_bool for band, _bool in sorted(zip(band, categorical))]
        for index, data_array in enumerate(data_arrays):
            data_arrays[index] = _resample_cellsize(
                data_array=data_array, categorical=categorical[index]
            )
    if _check_shape_and_extent(data_arrays[0]):
        for index, data_array in enumerate(data_arrays):
            data_arrays[index] = _to_bng_extent(
                data_array=data_array,
                nodata=nodata,
            )
    return merge(
        data_array.to_dataset(name=data_array.name) for data_array in data_arrays
    )
