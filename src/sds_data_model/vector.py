"""Vector wrapper classes."""
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Type, TypeVar

from affine import Affine
from dask.delayed import Delayed
from geopandas import GeoDataFrame
from graphviz import Digraph
from numpy import number, uint8
from numpy.typing import NDArray
from pandas import DataFrame, Series
from shapely.geometry import box
from xarray import DataArray, Dataset, merge

from sds_data_model._vector import (
    _check_layer_projection,
    _from_delayed_to_data_array,
    _from_file,
    _get_categories_and_dtypes,
    _get_gpdf,
    _get_info,
    _get_mask,
    _get_metadata,
    _get_name,
    _get_schema,
    _join,
    _recode_categorical_strings,
    _select,
    _to_raster,
    _where,
)
from sds_data_model.constants import (
    BBOXES,
    CELL_SIZE,
    OUT_SHAPE,
    BoundingBox,
    CategoryLookups,
    Schema,
)
from sds_data_model.graph import initialise_graph, update_graph
from sds_data_model.logger import log
from sds_data_model.metadata import Metadata

_VectorTile = TypeVar("_VectorTile", bound="VectorTile")


@dataclass
class VectorTile:
    """#TODO VectorTile class documentation."""

    bbox: BoundingBox
    gpdf: Delayed

    @property
    def transform(self: _VectorTile) -> Affine:
        """# TODO.

        Returns:
            Affine: _description_
        """
        xmin, _, _, ymax = self.bbox
        return Affine(CELL_SIZE, 0, xmin, 0, -CELL_SIZE, ymax)

    @classmethod
    def from_file(
        cls: Type[_VectorTile],
        data_path: str,
        bbox: BoundingBox,
        convert_to_categorical: Optional[List[str]] = None,
        category_lookups: Optional[CategoryLookups] = None,
        data_kwargs: Optional[Dict[str, Any]] = None,
    ) -> _VectorTile:
        """#TODO.

        Args:
            data_path (str): _description_
            bbox (BoundingBox): _description_
            convert_to_categorical (Optional[List[str]], optional): _description_.
                Defaults to None.
            category_lookups (Optional[CategoryLookups], optional): _description_.
                Defaults to None.
            data_kwargs (Optional[Dict[str, Any]], optional): _description_. Defaults
                to None.

        Returns:
            _VectorTile: _description_
        """
        gpdf = _from_file(
            data_path=data_path,
            bbox=bbox,
            convert_to_categorical=convert_to_categorical,
            category_lookups=category_lookups,
            data_kwargs=data_kwargs,
        )

        return cls(
            bbox=bbox,
            gpdf=gpdf,
        )

    def select(self: _VectorTile, columns: List[str]) -> _VectorTile:
        """# TODO.

        Args:
            columns (List[str]): _description_

        Returns:
            _VectorTile: _description_
        """
        self.gpdf = _select(gpdf=self.gpdf, columns=columns)
        return self

    def where(self: _VectorTile, condition: Series) -> _VectorTile:
        """# TODO.

        Args:
            condition (Series): _description_

        Returns:
            _VectorTile: _description_
        """
        self.gpdf = _where(gpdf=self.gpdf, condition=condition)
        return self

    def join(
        self: _VectorTile,
        other: DataFrame,
        how: str,
        fillna: Optional[Dict[str, Any]] = None,
        **kwargs: Dict[str, Any],
    ) -> _VectorTile:
        """# TODO.

        Args:
            other (DataFrame): _description_
            how (str): _description_
            fillna (Optional[Dict[str, Any]], optional): _description_. Defaults to
                None.
            **kwargs (Dict[str, Any]): _description_.

        Returns:
            _VectorTile: _description_
        """
        self.gpdf = _join(
            gpdf=self.gpdf,
            other=other,
            how=how,
            fillna=fillna,
            **kwargs,
        )
        return self

    def to_mask(
        self: _VectorTile,
        out_shape: Tuple[int, int] = OUT_SHAPE,
        invert: bool = True,
        dtype: str = "uint8",
    ) -> NDArray[uint8]:
        """# TODO.

        Args:
            out_shape (Tuple[int, int]): _description_. Defaults to OUT_SHAPE.
            invert (bool): _description_. Defaults to True.
            dtype (str): _description_. Defaults to "uint8".

        Returns:
            NDArray[uint8]: _description_
        """
        mask: NDArray[uint8] = _get_mask(
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
    ) -> NDArray[number]:
        """# TODO.

        Args:
            column (str): _description_
            out_shape (Tuple[int, int]): _description_. Defaults to OUT_SHAPE.
            dtype (str): _description_. Defaults to "uint8".

        Returns:
            NDArray[number]: _description_
        """
        raster: NDArray[number] = _to_raster(
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
    """# TODO."""

    name: str
    tiles: Tuple[VectorTile, ...]
    schema: Schema
    metadata: Optional[Metadata] = None
    category_lookups: Optional[CategoryLookups] = None
    graph: Optional[Digraph] = None

    @classmethod
    @log
    def from_files(
        cls: Type[_TiledVectorLayer],
        data_path: str,
        bboxes: Tuple[BoundingBox, ...] = BBOXES,
        data_kwargs: Optional[Dict[str, Any]] = None,
        convert_to_categorical: Optional[List[str]] = None,
        metadata_path: Optional[str] = None,
        name: Optional[str] = None,
    ) -> _TiledVectorLayer:
        """# TODO.

        Args:
            data_path (str): _description_
            bboxes (Tuple[BoundingBox, ...]): _description_. Defaults to BBOXES.
            data_kwargs (Optional[Dict[str, Any]], optional): _description_. Defaults
                to None.
            convert_to_categorical (Optional[List[str]], optional): _description_.
                Defaults to None.
            metadata_path (Optional[str], optional): _description_. Defaults to None.
            name (Optional[str], optional): _description_. Defaults to None.

        Returns:
            _TiledVectorLayer: _description_
        """
        info = _get_info(
            data_path=data_path,
            data_kwargs=data_kwargs,
        )

        _check_layer_projection(info)

        schema = _get_schema(info)

        metadata = _get_metadata(
            data_path=data_path,
            metadata_path=metadata_path,
        )

        _name = _get_name(
            name=name,
            metadata=metadata,
        )

        if convert_to_categorical:
            category_lookups, dtype_lookup = _get_categories_and_dtypes(
                data_path=data_path,
                convert_to_categorical=convert_to_categorical,
                data_kwargs=data_kwargs,
            )
            tiles = tuple(
                VectorTile.from_file(
                    data_path=data_path,
                    bbox=bbox,
                    convert_to_categorical=convert_to_categorical,
                    category_lookups=category_lookups,
                    data_kwargs=data_kwargs,
                )
                for bbox in bboxes
            )
            schema = {**schema, **dtype_lookup}
        else:
            category_lookups = None
            tiles = tuple(
                VectorTile.from_file(
                    data_path=data_path,
                    bbox=bbox,
                    data_kwargs=data_kwargs,
                )
                for bbox in bboxes
            )

        return cls(
            name=_name,
            tiles=tiles,
            metadata=metadata,
            category_lookups=category_lookups,
            schema=schema,
        )

    @log
    def select(self: _TiledVectorLayer, columns: List[str]) -> _TiledVectorLayer:
        """# TODO.

        Args:
            columns (List[str]): _description_

        Returns:
            _TiledVectorLayer: _description_
        """
        self.tiles = tuple(tile.select(columns) for tile in self.tiles)

        return self

    @log
    def where(self: _TiledVectorLayer, condition: Series) -> _TiledVectorLayer:
        """# TODO.

        Args:
            condition (Series): _description_

        Returns:
            _TiledVectorLayer: _description_
        """
        self.tiles = tuple(tile.where(condition) for tile in self.tiles)

        return self

    @log
    def join(
        self: _TiledVectorLayer,
        other: DataFrame,
        how: str = "left",
        fillna: Optional[Dict[str, Any]] = None,
        **kwargs: Dict[str, Any],
    ) -> _TiledVectorLayer:
        """# TODO.

        Args:
            other (DataFrame): _description_
            how (str): _description_. Defaults to "left".
            fillna (Optional[Dict[str, Any]], optional): _description_. Defaults to
                None.
            **kwargs (Dict[str, Any]): _description_.

        Returns:
            _TiledVectorLayer: _description_
        """
        self.tiles = tuple(
            tile.join(
                other=other,
                how=how,
                fillna=fillna,
                **kwargs,
            )
            for tile in self.tiles
        )

        # Update schema to include columns names and dtypes from dataframe being joined
        schema_df = other.dtypes.apply(lambda col: col.name).to_dict()
        schema = self.schema
        schema.update(schema_df)
        self.schema = schema

        return self

    @log
    def to_data_array_as_mask(self: _TiledVectorLayer) -> DataArray:
        """# TODO.

        Returns:
            DataArray: _description_
        """
        delayed_masks = tuple(tile.to_mask() for tile in self.tiles)

        data_array = _from_delayed_to_data_array(
            delayed_arrays=delayed_masks,
            name=self.name,
            metadata=self.metadata,
            dtype="uint8",
        )

        return data_array

    @log
    def to_dataset_as_raster(
        self: _TiledVectorLayer,
        columns: List[str],
    ) -> Dataset:
        """# TODO.

        This method rasterises the specified columns using a schema defined in
        VectorLayer. If columns have been specified as categorical by the user it
        updates the schema to uint32.

        Args:
            columns (List[str]): _description_

        Returns:
            Dataset: _description_
        """
        delayed_rasters = tuple(
            tuple(tile.to_raster(column=i, dtype=self.schema[i]) for tile in self.tiles)
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

        return dataset


_VectorLayer = TypeVar("_VectorLayer", bound="VectorLayer")


@dataclass
class VectorLayer:
    """# TODO."""

    name: str
    gpdf: GeoDataFrame
    schema: Schema
    graph: Digraph
    metadata: Optional[Metadata] = None
    category_lookups: Optional[CategoryLookups] = None

    @classmethod
    @log
    def from_files(
        cls: Type[_VectorLayer],
        data_path: str,
        data_kwargs: Optional[Dict[str, Any]] = None,
        convert_to_categorical: Optional[List[str]] = None,
        metadata_path: Optional[str] = None,
        name: Optional[str] = None,
    ) -> _VectorLayer:
        """# TODO.

        Args:
            data_path (str): _description_
            data_kwargs (Optional[Dict[str, Any]], optional): _description_. Defaults
                to None.
            convert_to_categorical (Optional[List[str]], optional): _description_.
                Defaults to None.
            metadata_path (Optional[str], optional): _description_. Defaults to None.
            name (Optional[str], optional): _description_. Defaults to None.

        Returns:
            _VectorLayer: _description_
        """
        info = _get_info(
            data_path=data_path,
            data_kwargs=data_kwargs,
        )

        _check_layer_projection(info)

        schema = _get_schema(info)

        metadata = _get_metadata(
            data_path=data_path,
            metadata_path=metadata_path,
        )

        _name = _get_name(
            name=name,
            metadata=metadata,
        )

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

        graph = initialise_graph(
            data_path=data_path,
            metadata_path=metadata_path,
            class_name="VectorLayer",
        )

        return cls(
            name=_name,
            gpdf=gpdf,
            metadata=metadata,
            category_lookups=category_lookups,
            schema=schema,
            graph=graph,
        )

    @log
    def to_tiles(
        self: _VectorLayer,
        bboxes: Tuple[BoundingBox, ...] = BBOXES,
    ) -> TiledVectorLayer:
        """# TODO.

        Args:
            bboxes (Tuple[BoundingBox, ...], optional): _description_. Defaults to
                BBOXES.

        Returns:
            TiledVectorLayer: _description_
        """
        tiles = tuple(
            VectorTile(
                bbox=bbox,
                gpdf=self.gpdf.clip(box(*bbox)),
            )
            for bbox in bboxes
        )

        graph = update_graph(
            graph=self.graph,
            method="to_tiles",
            output_class_name="TiledVectorLayer",
        )

        return TiledVectorLayer(
            name=self.name,
            tiles=tiles,
            metadata=self.metadata,
            category_lookups=self.category_lookups,
            schema=self.schema,
            graph=graph,
        )
