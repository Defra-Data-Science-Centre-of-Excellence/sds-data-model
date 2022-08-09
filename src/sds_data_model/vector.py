from dataclasses import dataclass
from logging import INFO, info, basicConfig
from typing import Any, Dict, Generator, List, Optional, TypeVar, Tuple

from affine import Affine
from dask.delayed import Delayed
from geopandas import GeoDataFrame
from pandas import DataFrame, Series
from shapely.geometry import box
from xarray import DataArray, Dataset, merge

from sds_data_model._vector import (
    _from_delayed_to_data_array,
    _from_file,
    _get_mask,
    _join,
    _select,
    _to_raster,
    _where,
    _get_schema,
    _get_categories_and_dtypes,
    _recode_categorical_strings,
    _get_info,
    _check_layer_projection,
    _get_gpdf,
)
from sds_data_model.constants import (
    BBOXES,
    CELL_SIZE,
    BoundingBox,
    OUT_SHAPE,
    CategoryLookups,
    Schema,
)
from sds_data_model.metadata import Metadata


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

    def select(self: _VectorTile, columns: List[str]) -> _VectorTile:
        gpdf = _select(gpdf=self.gpdf, columns=columns)
        return VectorTile(
            bbox=self.bbox,
            gpdf=gpdf,
        )

    def where(self: _VectorTile, condition: Series) -> _VectorTile:
        gpdf = _where(gpdf=self.gpdf, condition=condition)
        return VectorTile(
            bbox=self.bbox,
            gpdf=gpdf,
        )

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


_TiledVectorLayer = TypeVar("_TiledVectorLayer", bound="TiledVectorLayer")


@dataclass
class TiledVectorLayer:
    name: str
    tiles: Generator[VectorTile, None, None]
    metadata: Optional[Metadata]
    category_lookups: Optional[CategoryLookups]
    schema: Schema

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

        if not metadata_path:
            metadata = None
        else:
            metadata = Metadata.from_file(metadata_path)

        info(f"Read metadata from {metadata_path}.")

        _name = name if name else metadata["title"]

        schema = _get_schema(data_path=data_path, **data_kwargs)

        return cls(
            name=_name,
            tiles=tiles,
            metadata=metadata,
            schema=schema,
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
        """This method rasterises the specified columns using a schema defined in VectorLayer.
        If columns have been specified as categorical by the user it updates the schema to uint32."""

        tiles = list(self.tiles)

        delayed_rasters = (
            (tile.to_raster(column=i, dtype=self.schema[i]) for tile in tiles)
            for i in columns
        )

        dataset = merge(
            [
                _from_delayed_to_data_array(
                    delayed_arrays=i,
                    name=j,
                    metadata=self.metadata,
                    dtype=self.schema[j],
                )
                for i, j in zip(delayed_rasters, columns)
            ]
        )

        info(f"Converted to Dataset as raster.")

        return dataset


_VectorLayer = TypeVar("_VectorLayer", bound="VectorLayer")


@dataclass
class VectorLayer:
    name: str
    gpdf: GeoDataFrame
    schema: Schema
    metadata: Optional[Metadata] = None
    category_lookups: Optional[CategoryLookups] = None

    @classmethod
    def from_files(
        cls: _VectorLayer,
        data_path: str,
        data_kwargs: Optional[Dict[str, Any]] = None,
        convert_to_categorical: List[str] = None,
        metadata_path: Optional[str] = None,
        name: Optional[str] = None,
        schema: Optional[Dict[str, Any]] = None
    ) -> _VectorLayer:
        info = _get_info(
            data_path=data_path,
            data_kwargs=data_kwargs,
        )

        _check_layer_projection(info)

        if schema is not None:
            schema = schema
        else:
            schema = _get_schema(info)

        if not metadata_path:
            metadata = None
        else:
            metadata = Metadata.from_file(metadata_path)

        _name = name if name else metadata["title"]

        gpdf = _get_gpdf(
            data_path=data_path,
            data_kwargs=data_kwargs,            
        )

        if convert_to_categorical:
            category_lookups, dtype_lookup = _get_categories_and_dtypes(
                data_path=data_path,
                convert_to_categorical=convert_to_categorical,
                data_kwargs=data_kwargs,
            )
            for column in convert_to_categorical:
                gpdf = _recode_categorical_strings(
                    gpdf=gpdf,
                    column=column,
                    category_lookups=category_lookups,
                )
            schema = {**schema, **dtype_lookup}
        else:
            category_lookups = None

        return cls(
            name=_name,
            gpdf=gpdf,
            metadata=metadata,
            category_lookups=category_lookups,
            schema=schema,
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
            category_lookups=self.category_lookups,
            schema=self.schema,
        )
