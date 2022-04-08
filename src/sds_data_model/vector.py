from asyncio import gather
from dataclasses import asdict, dataclass
import json
from typing import Generator, Optional, TypeVar, Tuple

from affine import Affine
from geopandas import GeoDataFrame, read_file
from shapely.geometry import box

from sds_data_model._vector import _get_mask
from sds_data_model.constants import BBOXES, CELL_SIZE, BNG, BoundingBox, OUT_SHAPE
from sds_data_model.metadata import Metadata
from sds_data_model.raster import MaskTile, TiledMaskLayer


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
    ) -> MaskTile:
        mask_array = _get_mask(
            gpdf=self.gpdf,
            out_shape=out_shape,
            transform=self.transform,
            invert=invert,
            dtype=dtype,
        )

        return MaskTile(bbox=self.bbox, array=mask_array)


_TiledVectorLayer = TypeVar("_TiledVectorLayer", bound="TiledVectorLayer")


@dataclass
class TiledVectorLayer:
    name: str
    tiles: Generator[VectorTile, None, None]
    metadata: Metadata

    def to_masks(self: _TiledVectorLayer) -> TiledMaskLayer:
        masks = (tile.to_mask() for tile in self.tiles)
        return TiledMaskLayer(
            name=self.name,
            masks=masks,
            metadata=self.metadata,
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
