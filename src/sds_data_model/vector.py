"""Vector wrapper classes."""
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Type, TypeVar

from affine import Affine
from dask.delayed import Delayed
from geopandas import GeoDataFrame
from numpy import number, uint8
from numpy.typing import NDArray
