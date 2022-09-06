from affine import Affine
from cv2 import INTER_LINEAR_EXACT, INTER_NEAREST_EXACT, resize
from numpy import array, ndarray, full
from typing import Optional
from xarray import DataArray, Dataset

from sds_data_model.constants import BNG_XMAX, BNG_XMIN, BNG_YMAX, CELL_SIZE

def _check_cellsize(
    transform: Affine,
) -> Optional[bool]:
    if (
        transform.a != CELL_SIZE or 
        -transform.e != CELL_SIZE
    ):
        return True
    else:
        return None


def _check_shape_and_extent(
    raster: Dataset,
) -> Optional[bool]:
    if (
        raster.rio.shape != (BNG_YMAX/CELL_SIZE, BNG_XMAX/CELL_SIZE) or 
        raster.rio.transform().f != BNG_YMAX or 
        raster.rio.transform().c != BNG_XMIN
    ):
        return True
    else:
        return None
    
def _data_array_to_dataset(
    raster: DataArray,
    data: ndarray,
    geotransform: str,
):
    return DataArray(
        data=data,
        coords={
            raster.rio.grid_mapping : 
            raster[raster.rio.grid_mapping].rio.update_attrs(
                {"GeoTransform" : geotransform}
            )
        },
        dims=tuple(raster.dims),
    ).to_dataset(name=raster.coords['variable'].data.tolist())
    
        
def _get_resample_shape(
    raster: DataArray,
) -> ndarray:
    return (
        (
            array(
                [raster.rio.transform().a, -raster.rio.transform().e]
            ) / CELL_SIZE
        ) * raster.shape
    ).round().astype("int")


def _resample_cellsize(
    raster: DataArray,
    categorical: bool=False,
) -> Dataset:
    if categorical:
        interpolation=INTER_NEAREST_EXACT
    else:
        interpolation=INTER_LINEAR_EXACT
    resampled = resize(
        src=raster.data,
        dsize=_get_resample_shape(raster)[::-1],
        interpolation=interpolation,
    )
    
    return _data_array_to_dataset(
        raster=raster,
        data=resampled,
        geotransform=f"{raster.rio.transform().c} {CELL_SIZE} 0 {raster.rio.transform().f} 0 -{CELL_SIZE}",
    )

def _check_no_data(
    raster: DataArray,
    nodata: float=None,
) -> None:
    if nodata is not None:
        raster.rio.write_nodata(nodata, inplace=True)
    else:
        assert raster.rio.nodata is not None, "Input dataset does not have a nodata value. One must be provided."
    

def _get_bng_offset(
    raster: DataArray,
) -> ndarray:
    return (
        array(
            [BNG_YMAX - raster.rio.transform().f, raster.rio.transform().c]
        ) / CELL_SIZE
    ).round().astype("int")


def _to_bng_extent(
    raster: DataArray,
    nodata: float=None
) -> Dataset:
    _check_no_data(raster, nodata)
    
    bng = full(
        shape=(
            array(
                [BNG_YMAX, BNG_XMAX]
            ) / CELL_SIZE
        ).astype("int"),
        fill_value=raster.rio.nodata,
        dtype=raster.dtype   
    )

    offset = _get_bng_offset(raster)

    bng[
        offset[0]:offset[0] + raster.shape[0],
        offset[1]:offset[1] + raster.shape[1]
    ] = raster.data
    
    return _data_array_to_dataset(
        raster=raster,
        data=bng,
        geotransform=f"{BNG_XMIN} {CELL_SIZE} 0 {BNG_YMAX} 0 -{CELL_SIZE}",
    )

