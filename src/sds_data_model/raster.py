from dataclasses import dataclass
from typing import Optional, Tuple, TypeVar

from xarray import DataArray, Dataset, open_mfdataset

from sds_data_model.vector import TiledBngVectorLayer


BngRasterLayerType = TypeVar("BngRasterLayerType", bound="BngRasterLayer")


@dataclass
class BngRasterLayer:
    dataset: Dataset

    @classmethod
    def from_tiled_bng_vector_layer(
        cls: BngRasterLayerType,
        tiled_bng_vector_layer: TiledBngVectorLayer,
        path: Optional[str] = None,
        layer_name: Optional[str] = None,
    ) -> BngRasterLayerType:

        _layer_name = (
            layer_name if layer_name else tiled_bng_vector_layer.metadata.title
        )

        for vector_tile in tiled_bng_vector_layer.tiles:
            vector_tile.to_bng_raster_tile_netcdf(path)

        dataset = open_mfdataset(f"{path}/{_layer_name}/*.nc")

        return cls(
            dataset=dataset,
        )
