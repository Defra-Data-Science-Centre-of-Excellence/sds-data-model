from dataclasses import dataclass
import json
from logging import INFO, info, basicConfig
from typing import Any, Dict, Generator, List, Optional, TypeVar, Tuple, Union

from affine import Affine
from dask.delayed import Delayed
from geopandas import GeoDataFrame, read_file
from pandas import DataFrame, Series
from shapely.geometry import box
from xarray import DataArray, Dataset, merge

from sds_data_model._helpers import _read_metadata
from sds_data_model._vector import (
    _from_delayed_to_data_array,
    _from_file,
    _get_mask,
    _join,
    _select,
    _to_raster,
    _where,
    _get_col_dtype,
)
from sds_data_model.constants import (
    BBOXES,
    CELL_SIZE,
    BNG,
    BoundingBox,
    OUT_SHAPE,
    raster_dtype_levels,
)
from sds_data_model.metadata import Metadata
from sds_data_model.logger import log, graph


basicConfig(format="%(levelname)s:%(asctime)s:%(message)s", level=INFO)

_VectorTile = TypeVar("_VectorTile", bound="VectorTile")


@dataclass
class VectorTile:
    bbox: BoundingBox
    gpdf: Delayed

    @property
    def transform(self: _VectorTile) -> Affine:
        xmin, _, _, ymax = self.bbox
        return Affine(CELL_SIZE, 0, xmin, 0, -CELL_SIZE, ymax)

    @classmethod
    @log
    def from_file(
        cls: _VectorTile,
        data_path: str,
        bbox: BoundingBox,
        **kwargs: Dict[str, Any],
    ) -> _VectorTile:
        gpdf = _from_file(
            data_path=data_path,
            bbox=bbox,
            **kwargs,
        )

        return cls(
            bbox=bbox,
            gpdf=gpdf,
        )

    @log
    def select(self: _VectorTile, columns: List[str]) -> _VectorTile:
        gpdf = _select(gpdf=self.gpdf, columns=columns)
        return VectorTile(
            bbox=self.bbox,
            gpdf=gpdf,
        )

    @log
    def where(self: _VectorTile, condition: Series) -> _VectorTile:
        gpdf = _where(gpdf=self.gpdf, condition=condition)
        return VectorTile(
            bbox=self.bbox,
            gpdf=gpdf,
        )

    @log
    def join(
        self: _VectorTile,
        other: DataFrame,
        how: str,
        fillna: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> _VectorTile:
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

    @log
    def to_mask(
        self: _VectorTile,
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

    @log
    def to_raster(
        self: _VectorTile,
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

    @log
    def get_col_dtype(
        self: _VectorTile,
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
        cls: _TiledVectorLayer,
        data_path: str,
        data_kwargs: Dict[str, Any],
        bboxes: Tuple[BoundingBox] = BBOXES,
        metadata_path: Optional[str] = None,
        name: Optional[str] = None,
    ) -> _TiledVectorLayer:
        tiles = tuple(
            VectorTile.from_file(
                data_path=data_path,
                bbox=bbox,
                **data_kwargs,
            )
            for bbox in bboxes
        )

        info(f"Read data from {data_path}.")

        # ? How are we going to check the crs?
        # ? __post_init__ method on VectorTile instead?
        # if gpdf.crs.name != BNG:
        #     raise TypeError(f"CRS must be {BNG}, not {gpdf.crs.name}")

        metadata = _read_metadata(
            data_path=data_path,
            metadata_path=metadata_path,
        )

        info(f"Read metadata from {metadata_path}.")

        _name = name if name else metadata["title"]

        return cls(
            name=_name,
            tiles=tiles,
            metadata=metadata,
        )

    def select(self: _TiledVectorLayer, columns: List[str]) -> _TiledVectorLayer:
        tiles = tuple(tile.select(columns) for tile in self.tiles)

        tiled_vector_layer = TiledVectorLayer(
            name=self.name,
            tiles=tiles,
            metadata=self.metadata,
        )

        info(f"Selected columns: {columns}.")

        return tiled_vector_layer

    def where(self: _TiledVectorLayer, condition: Series) -> _TiledVectorLayer:
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
    ) -> _TiledVectorLayer:
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
    @graph
    def from_files(
        cls: _VectorLayer,
        data_path: str,
        data_kwargs: Dict[str, Any],
        metadata_path: Optional[str] = None,
        name: Optional[str] = None,
    ) -> _VectorLayer:
        gpdf = read_file(data_path, **data_kwargs)

        if gpdf.crs.name not in BNG:
            raise TypeError(f"CRS must be one of {BNG}, not {gpdf.crs.name}")

        metadata = _read_metadata(
            data_path=data_path,
            metadata_path=metadata_path,
        )

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
