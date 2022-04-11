from dataclasses import asdict, dataclass
import json
from typing import Generator, Optional, TypeVar, Tuple

from affine import Affine
from dask.array import block, from_delayed
from dask.delayed import Delayed
from geopandas import GeoDataFrame, read_file
from more_itertools import chunked
from numpy import arange
from shapely.geometry import box
from xarray import DataArray

from sds_data_model._vector import _get_mask
from sds_data_model.constants import (
    BBOXES,
    CELL_SIZE,
    BNG,
    BNG_XMIN,
    BNG_XMAX,
    BNG_YMIN,
    BNG_YMAX,
    BoundingBox,
    OUT_SHAPE,
)
from sds_data_model.metadata import Metadata

_VectorTile = TypeVar("_VectorTile", bound="VectorTile")


@dataclass
class VectorTile:
    bbox: BoundingBox
    gpdf: GeoDataFrame

    @property
    def transform(self: _VectorTile) -> Affine:
        xmin, _, _, ymax = self.bbox
        return Affine(CELL_SIZE, 0, xmin, 0, -CELL_SIZE, ymax)

    def to_mask(
        self: _VectorTile,
        out_shape: Tuple[int, int] = OUT_SHAPE,
        invert: bool = True,
        dtype: str = "uint8",
    ) -> Delayed:
        return _get_mask(
            gpdf=self.gpdf,
            out_shape=out_shape,
            transform=self.transform,
            invert=invert,
            dtype=dtype,
        )


_TiledVectorLayer = TypeVar("_TiledVectorLayer", bound="TiledVectorLayer")


@dataclass
class TiledVectorLayer:
    name: str
    tiles: Generator[VectorTile, None, None]
    metadata: Metadata

    def to_data_array_as_mask(self: _TiledVectorLayer) -> DataArray:
        delayed_masks = tuple(tile.to_mask() for tile in self.tiles)
        dask_arrays = tuple(
            from_delayed(mask, dtype="uint8", shape=(10_000, 10_000))
            for mask in delayed_masks
        )
        rows = chunked(dask_arrays, 7)
        data = block(list(rows))
        return DataArray(
            data=data,
            coords={
                "northings": ("northings", arange(BNG_YMAX, BNG_YMIN, -CELL_SIZE)),
                "eastings": ("eastings", arange(BNG_XMIN, BNG_XMAX, CELL_SIZE)),
            },
            name=self.name,
            attrs= asdict(self.metadata),
        )


_VectorLayer = TypeVar("_VectorLayer", bound="VectorLayer")


@dataclass
class VectorLayer:
    name: str
    gpdf: GeoDataFrame
    metadata: Metadata

    @classmethod
    def from_files(
        cls: _VectorLayer,
        data_path: str,
        metadata_path: Optional[str] = None,
        name: Optional[str] = None,
    ) -> _VectorLayer:
        gpdf = read_file(data_path)

        if gpdf.crs.name != BNG:
            raise TypeError(f"CRS must be {BNG}, not {gpdf.crs.name}")

        if not metadata_path:
            metadata = json.load(f"{data_path}-metadata.json")
        else:
            metadata = Metadata.from_file(metadata_path)

        _name = name if name else metadata["title"]

        return cls(
            name=_name,
            gpdf=gpdf,
            metadata=metadata,
        )

    def to_tiles(self, bboxes: Tuple[BoundingBox] = BBOXES) -> TiledVectorLayer:
        tiles = (
            VectorTile(
                bbox=bbox,
                gpdf=self.gpdf.clip(box(*bbox)),
            )
            for bbox in bboxes
        )

        return TiledVectorLayer(
            name=self.name,
            tiles=tiles,
            metadata=self.metadata,
        )
