from dataclasses import asdict, dataclass
import json
from pathlib import Path
from typing import Optional, TypeVar, Tuple

from affine import Affine
import attrs
from geopandas import clip, GeoDataFrame, read_file
from importlib_metadata import metadata
from matplotlib.pyplot import grid
from numpy import arange, ndarray, zeros, ones
from shapely.geometry import Polygon
from rasterio.features import geometry_mask
from xarray import DataArray

from sds_data_model.constants import BNG, GRID_SIZE, OUT_SHAPE
from sds_data_model.metadata import Metadata


@dataclass
class BngVectorTile:
    name: str
    bbox: Polygon
    data: GeoDataFrame
    metadata: Metadata

    def to_mask(
        self,
        out_shape: Tuple[int, int],
        transform: Affine,
        dtype: str = "uint8",
        invert: bool = True,
    ) -> ndarray:
        if all(self.data.geometry.is_empty) and invert:
            return zeros(
                shape=out_shape,
                dtype=dtype,
            )
        elif all(self.data.geometry.is_empty) and not invert:
            return ones(
                shape=out_shape,
                dtype=dtype,
            )
        else:
            return geometry_mask(
                geometries=self.data.geometry,
                out_shape=out_shape,
                transform=transform,
                invert=invert,
            ).astype(dtype)

    def to_netcdf_as_mask(
        self,
        path: str,
        layer_name: str,
    ) -> None:
        xmin, ymin, xmax, ymax = self.bbox

        transform = Affine(
            GRID_SIZE,
            0,
            xmin,
            0,
            -GRID_SIZE,
            ymax,
        )

        mask = self.to_mask(
            out_shape=OUT_SHAPE,
            transform=transform,
        )

        data_array = DataArray(
            data=mask,
            dims=("x", "y"),
            coords={
                "northings": ("y", arange(ymax, ymin, -GRID_SIZE)),
                "eastings": ("x", arange(xmin, xmax, GRID_SIZE)),
            },
            name=layer_name,
            attrs=asdict(self.metadata),
        )

        out_path = Path(path) / layer_name

        if not out_path.exists():
            out_path.mkdir(parents=True)

        data_array.to_netcdf(path=f"{str(out_path)}/{self.name}.nc")


TiledBngVectorLayerType = TypeVar(
    "TiledBngVectorLayerType", bound="TiledBngVectorLayer"
)


@dataclass
class TiledBngVectorLayer:
    name: str
    tiles: Tuple[BngVectorTile]
    metadata: Metadata

    def to_netcdf_as_mask(
        self: TiledBngVectorLayerType,
        path: Optional[str] = None,
    ) -> None:
        _path = path if path else "."
        for vector_tile in self.tiles:
            vector_tile.to_netcdf_as_mask(
                path=_path,
                layer_name=self.name,
            )


BngVectorLayerType = TypeVar("BngVectorLayerType", bound="BngVectorLayer")


@dataclass
class BngVectorLayer:
    name: str
    data: GeoDataFrame
    metadata: Metadata

    @classmethod
    def from_files(
        cls: BngVectorLayerType,
        data_path: str,
        metadata_path: Optional[str] = None,
        name: Optional[str] = None,
    ) -> BngVectorLayerType:
        data = read_file(data_path)

        if data.crs.name != BNG:
            raise TypeError(f"CRS must be {BNG}, not {data.crs.name}")

        if not metadata_path:
            metadata = json.load(f"{data_path}-metadata.json")
        else:
            metadata = Metadata.from_file(metadata_path)

        _name = name if name else metadata["title"]

        return cls(
            name=_name,
            data=data,
            metadata=metadata,
        )

    def to_tiles(self, grid_path: str, grid_layer: str) -> TiledBngVectorLayer:
        grid = read_file(
            grid_path,
            layer=grid_layer,
        )

        if grid.crs.name != BNG:
            raise TypeError(f"CRS must be {BNG}, not {grid.crs.name}")

        tiles = tuple(
            BngVectorTile(
                name=data["tile_name"],
                bbox=data["geometry"].bounds,
                data=clip(self.data, data["geometry"]),
                metadata=self.metadata,
            )
            for _, data in grid.iterrows()
        )

        return TiledBngVectorLayer(
            name=self.name,
            tiles=tiles,
            metadata=self.metadata,
        )
