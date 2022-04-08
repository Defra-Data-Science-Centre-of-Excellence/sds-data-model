from dataclasses import dataclass
from pathlib import Path
from typing import Generator

from fsspec import get_mapper
from numpy import arange, ndarray
from xarray import DataArray

from sds_data_model.constants import CELL_SIZE, BoundingBox
from sds_data_model.metadata import Metadata


@dataclass
class TiledDataArrayLayer:
    name: str
    data_arrays: Generator[DataArray, None, None]
    metadata: Metadata

    def to_netcdfs(self, root: str) -> None:
        base_path = Path(root) / self.name
        if root.startswith("s3://"):
            s3_path = get_mapper(str(base_path), s3_additional_kwargs={'ACL': 'bucket-owner-full-control'})
        else:
            if not base_path.exists():
                base_path.mkdir(parents=True)
        for index, data_array in enumerate(self.data_arrays):
            out_path = base_path / f"{index}.nc"
            data_array.to_netcdf(out_path)


@dataclass
class MaskTile:
    bbox: BoundingBox
    array: ndarray

    def to_data_array(self, name: str) -> DataArray:
        xmin, ymin, xmax, ymax = self.bbox
        return DataArray(
            data=self.array,
            coords={
                "northings": ("northings", arange(ymax, ymin, -CELL_SIZE)),
                "eastings": ("eastings", arange(xmin, xmax, CELL_SIZE)),
            },
            name=name,
        )


@dataclass
class TiledMaskLayer:
    name: str
    masks: Generator[MaskTile, None, None]
    metadata: Metadata

    def to_data_arrays(self) -> TiledDataArrayLayer:
        data_arrays = (mask.to_data_array(name=self.name) for mask in self.masks)

        return TiledDataArrayLayer(
            name=self.name,
            data_arrays=data_arrays,
            metadata=self.metadata,
        )
