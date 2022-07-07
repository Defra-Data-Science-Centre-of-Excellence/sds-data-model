import json
from dataclasses import dataclass
from logging import INFO, basicConfig, info
from typing import Any, Dict, Generator, List, Optional, Tuple, TypeVar, Type

from affine import Affine
from dask.delayed import Delayed
from geopandas import GeoDataFrame, read_file
from pandas import DataFrame, Series
from shapely.geometry import box
from xarray import DataArray, Dataset, merge

from sds_data_model._vector import (
    _from_delayed_to_data_array,
    _from_file,
    _get_col_dtype,
    _get_mask,
    _join,
    _select,
    _to_raster,
    _where,
)
from sds_data_model.constants import (
    BBOXES,
    BNG,
    CELL_SIZE,
    OUT_SHAPE,
    BoundingBox,
    raster_dtype_levels,
)
from sds_data_model.metadata import Metadata

basicConfig(format="%(levelname)s:%(asctime)s:%(message)s", level=INFO)


_VectorTileType = TypeVar("_VectorTileType", bound="VectorTile")

@dataclass
class VectorTile:
    bbox: BoundingBox
    gpdf: Delayed


    @property
    def transform(self) -> Affine:
        xmin, _, _, ymax = self.bbox
        return Affine(CELL_SIZE, 0, xmin, 0, -CELL_SIZE, ymax)

    @classmethod
    def from_file(
        cls: Type[_VectorTileType],
        data_path: str,
        bbox: BoundingBox,
        **kwargs: Dict[str, Any],
    ) -> "VectorTile":
        gpdf = _from_file(
            data_path=data_path,
            bbox=bbox,
            **kwargs,
        )

        return cls(
            bbox=bbox,
            gpdf=gpdf,
        )

    def select(self: _VectorTileType, columns: List[str]) -> "VectorTile":
        gpdf = _select(gpdf=self.gpdf, columns=columns)
        return VectorTile(
            bbox=self.bbox,
            gpdf=gpdf,
        )

    def where(self: _VectorTileType, condition: Series) -> "VectorTile":
        gpdf = _where(gpdf=self.gpdf, condition=condition)
        return VectorTile(
            bbox=self.bbox,
            gpdf=gpdf,
        )

    def join(
        self: _VectorTileType,
        other: DataFrame,
        how: str,
        fillna: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> "VectorTile":
        gpdf = _join(
            gpdf=self.gpdf,
            other=other,
            how=how,
            fillna=fillna,
            **kwargs,
        )
        return VectorTile(
            bbox=self.bbox,
            gpdf=gpdf,
        )

    def to_mask(
        self: _VectorTileType,
        out_shape: Tuple[int, int] = OUT_SHAPE,
        invert: bool = True,
        dtype: str = "uint8",
    ) -> Delayed:
        mask = _get_mask(
            gpdf=self.gpdf,
            out_shape=out_shape,
            transform=self.transform,
            invert=invert,
            dtype=dtype,
        )

        return mask

    def to_raster(
        self: _VectorTileType,
        column: str,
        out_shape: Tuple[int, int] = OUT_SHAPE,
        dtype: str = "uint8",
    ) -> Delayed:
        raster = _to_raster(
            gpdf=self.gpdf,
            column=column,
            out_shape=out_shape,
            transform=self.transform,
            dtype=dtype,
        )

        return raster

    def get_col_dtype(
        self: _VectorTileType,
        column: str,
    ) -> str:
        """This method calls _get_col_dtype on an individual vectortile."""
        return _get_col_dtype(
            gpdf=self.gpdf,
            column=column,
        )


_TiledVectorLayer = TypeVar("_TiledVectorLayer", bound="TiledVectorLayer")


@dataclass
class TiledVectorLayer:
    name: str
    tiles: Generator[VectorTile, None, None]
    metadata: Optional[Metadata]

    @classmethod
    def from_files(
        cls: Type[_TiledVectorLayer],
        data_path: str,
        bboxes: Tuple[BoundingBox, ...] = BBOXES,
        data_kwargs: Optional[Dict[str, Any]] = None,
        metadata_path: Optional[str] = None,
        name: Optional[str] = None,
    ) -> "TiledVectorLayer":
        if data_kwargs:
            tiles = (
                VectorTile.from_file(
                    data_path=data_path,
                    bbox=bbox,
                    **data_kwargs,
                )
                for bbox in bboxes
            )
        else:
            tiles = (
                VectorTile.from_file(
                    data_path=data_path,
                    bbox=bbox,
                )
                for bbox in bboxes
            )

        info(f"Read data from {data_path}.")

        # ? How are we going to check the crs?
        # ? __post_init__ method on VectorTile instead?
        # if gpdf.crs.name != BNG:
        #     raise TypeError(f"CRS must be {BNG}, not {gpdf.crs.name}")

        if not metadata_path:
            metadata = None
        else:
            metadata = Metadata.from_file(metadata_path)

        info(f"Read metadata from {metadata_path}.")

        _name = name if name else metadata["title"]

        return cls(
            name=_name,
            tiles=tiles,
            metadata=metadata,
        )

    def select(self: _TiledVectorLayer, columns: List[str]) -> "TiledVectorLayer":
        tiles = tuple(tile.select(columns) for tile in self.tiles)

        tiled_vector_layer = TiledVectorLayer(
            name=self.name,
            tiles=tiles,
            metadata=self.metadata,
        )

        info(f"Selected columns: {columns}.")

        return tiled_vector_layer

    def where(self: _TiledVectorLayer, condition: Series) -> "TiledVectorLayer":
        tiles = tuple(tile.where(condition) for tile in self.tiles)

        tiled_vector_layer = TiledVectorLayer(
            name=self.name,
            tiles=tiles,
            metadata=self.metadata,
        )

        info(f"Selected rows where {condition}.")

        return tiled_vector_layer

    def join(
        self: _TiledVectorLayer,
        other: DataFrame,
        how: str = "left",
        fillna: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> "TiledVectorLayer":
        tiles = tuple(
            tile.join(
                other=other,
                how=how,
                fillna=fillna,
                **kwargs,
            )
            for tile in self.tiles
        )

        tiled_vector_layer = TiledVectorLayer(
            name=self.name,
            tiles=tiles,
            metadata=self.metadata,
        )

        info(f"Joined to {other.info()}.")

        return tiled_vector_layer

    def to_data_array_as_mask(self: _TiledVectorLayer) -> DataArray:
        delayed_masks = (tile.to_mask() for tile in self.tiles)

        data_array = _from_delayed_to_data_array(
            delayed_arrays=delayed_masks,
            name=self.name,
            metadata=self.metadata,
        )

        info(f"Converted to DataArray as mask.")

        return data_array

    def to_dataset_as_raster(
        self: _TiledVectorLayer,
        columns: List[str],
    ) -> Dataset:
        """This method instantiates the tiles so that they can be used in subsequent methods.
        get_col_dtypes is called on all vectortiles, removes None (due to tiles on irish sea),
        Gets the index location of each dtype from the constant list and returns the maximum index
        location (most complex data type) to accomodate all tiles in the layer, for every column provided"""

        tiles = list(self.tiles)

        col_dtypes = ((tile.get_col_dtype(column=i) for tile in tiles) for i in columns)

        col_dtypes_clean = [[x for x in list(i) if x is not None] for i in col_dtypes]
        col_dtypes_levels = [
            [raster_dtype_levels.index(x) for x in i] for i in col_dtypes_clean
        ]
        dtype = [raster_dtype_levels[max(i)] for i in col_dtypes_levels]

        delayed_rasters = (
            (tile.to_raster(column=i, dtype=j) for tile in tiles)
            for i, j in zip(columns, dtype)
        )

        dataset = merge(
            [
                _from_delayed_to_data_array(
                    delayed_arrays=i,
                    name=j,
                    metadata=self.metadata,
                    dtype=k,
                )
                for i, j, k in zip(delayed_rasters, columns, dtype)
            ]
        )

        info(f"Converted to Dataset as raster.")

        return dataset


_VectorLayer = TypeVar("_VectorLayer", bound="VectorLayer")


@dataclass
class VectorLayer:
    name: str
    gpdf: GeoDataFrame
    metadata: Optional[Metadata]

    @classmethod
    def from_files(
        cls: Type[_VectorLayer],
        data_path: str,
        data_kwargs: Optional[Dict[str, Any]] = None,
        metadata_path: Optional[str] = None,
        name: Optional[str] = None,
    ) -> "VectorLayer":
        if data_kwargs:
            gpdf = read_file(data_path, **data_kwargs)
        else:
            gpdf = read_file(data_path)

        if gpdf.crs.name not in BNG:
            raise TypeError(f"CRS must be one of {BNG}, not {gpdf.crs.name}")

        if not metadata_path:
            metadata = None
        else:
            metadata = Metadata.from_file(metadata_path)

        _name = name if name else metadata["title"]

        return cls(
            name=_name,
            gpdf=gpdf,
            metadata=metadata,
        )

    def select(self: _VectorLayer, columns: List[str]) -> "VectorLayer":
        gpdf = _select(gpdf=self.gpdf, columns=columns)
        return VectorLayer(
            gpdf=gpdf,
            name=self.name,
            metadata=self.metadata,
        )

    def where(self: _VectorLayer, condition: Series) -> "VectorLayer":
        gpdf = _where(self.gpdf, condition=condition)
        return VectorLayer(
            gpdf=gpdf,
            name=self.name,
            metadata=self.metadata,
        )

    def to_tiles(self, bboxes: Tuple[BoundingBox, ...] = BBOXES) -> TiledVectorLayer:
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
