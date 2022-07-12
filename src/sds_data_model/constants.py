from itertools import product
from typing import Tuple

# British National Grid (BNG) name strings
BNG = ("OSGB 1936 / British National Grid", "OSGB36 / British National Grid")
# Minimum x value of BNG in meters
BNG_XMIN = 0
# Minimum y value of BNG in meters
BNG_YMIN = 0
# Maximum x value of BNG in meters
BNG_XMAX = 700_000
# Maximum y value of BNG in meters
BNG_YMAX = 1_300_000
# Height and width of bounding box in meters
BOX_SIZE = 100_000
# Height and width of raster cell in meters
CELL_SIZE = 10
# Height and width of VectorTile in cells
TILE_SIZE = BOX_SIZE // CELL_SIZE
# Dimensions of VectorTile in cells
OUT_SHAPE = (TILE_SIZE, TILE_SIZE)

TITLE_XPATH = [
    "gmd:identificationInfo",
    "gmd:MD_DataIdentification",
    "gmd:citation",
    "gmd:CI_Citation",
    "gmd:title",
    "gco:CharacterString",
]

DATASET_LANGUAGE_XPATH = [
    "gmd:identificationInfo",
    "gmd:MD_DataIdentification",
    "gmd:language",
    "gmd:LanguageCode",
]

TOPIC_CATEGORY_XPATH = [
    "gmd:identificationInfo",
    "gmd:MD_DataIdentification",
    "gmd:topicCategory",
    "gmd:MD_TopicCategoryCode",
]

KEYWORD_XPATH = [
    "gmd:identificationInfo",
    "gmd:MD_DataIdentification",
    "gmd:descriptiveKeywords",
    "gmd:MD_Keywords",
    "gmd:keyword",
    "gco:CharacterString",
]

QUALITY_SCOPE_XPATH = [
    "gmd:dataQualityInfo",
    "gmd:DQ_DataQuality",
    "gmd:scope",
    "gmd:DQ_Scope",
    "gmd:level",
    "gmd:MD_ScopeCode",
]

SPATIAL_REPRESENTATION_TYPE_XPATH = [
    "gmd:identificationInfo",
    "gmd:MD_DataIdentification",
    "gmd:spatialRepresentationType",
    "gmd:MD_SpatialRepresentationTypeCode",
]

# Type alias, i.e. a BoundingBox is a Tuple of 4 integers
# See https://docs.python.org/3.8/library/typing.html#type-aliases
BoundingBox = Tuple[int, int, int, int]

# Order for data types taken from rasterio docs lines 14-27
# https://github.com/rasterio/rasterio/blob/master/rasterio/dtypes.py
raster_dtype_levels = [
    "bool",
    "uint8",
    "int8",
    "uint16",
    "int16",
    "uint32",
    "int32",
    "float32",
    "float64",
    "complex",
    "complex64",
    "complex128",
]


def _get_bboxes(
    xmin: int = BNG_XMIN,
    ymin: int = BNG_YMIN,
    xmax: int = BNG_XMAX,
    ymax: int = BNG_YMAX,
    box_size: int = BOX_SIZE,
) -> Tuple[BoundingBox, ...]:
    """Returns a tuple of BoundingBox for BNG 100km grid squares."""
    eastings = range(xmin, xmax, box_size)
    northings = range(ymax, ymin, -box_size)

    top_left_coordinates = product(northings, eastings)

    return tuple((x, y - box_size, x + box_size, y) for y, x in top_left_coordinates)


# A tuple of BoundingBox for BNG 100km grid squares.
BBOXES = _get_bboxes()
