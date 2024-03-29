"""Constants."""
from itertools import product
from typing import Dict, Tuple

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

# Type alias, i.e. a BoundingBox is a Tuple of 4 integers
# See https://docs.python.org/3.8/library/typing.html#type-aliases
BoundingBox = Tuple[int, int, int, int]


CategoryLookup = Dict[int, str]
CategoryLookups = Dict[str, CategoryLookup]
Schema = Dict[str, str]

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
    """Returns a tuple of BoundingBox for BNG 100km grid squares.

    Args:
        xmin (int): # TODO. Defaults to BNG_XMIN.
        ymin (int): # TODO. Defaults to BNG_YMIN.
        xmax (int): # TODO. Defaults to BNG_XMAX.
        ymax (int): # TODO. Defaults to BNG_YMAX.
        box_size (int): # TODO. Defaults to BOX_SIZE.

    Returns:
        Tuple[BoundingBox, ...]: # TODO
    """
    eastings = range(xmin, xmax, box_size)
    northings = range(ymax, ymin, -box_size)

    top_left_coordinates = product(northings, eastings)

    return tuple((x, y - box_size, x + box_size, y) for y, x in top_left_coordinates)


# A tuple of BoundingBox for BNG 100km grid squares.
BBOXES = _get_bboxes()

# Numeric constants
INT8_MINIMUM = 2**8 // -2
INT8_MAXIMUM = (2**8 // 2) - 1
INT16_MINIMUM = 2**16 // -2
INT16_MAXIMUM = (2**16 // 2) - 1
INT32_MINIMUM = 2**32 // -2
INT32_MAXIMUM = (2**32 // 2) - 1
INT64_MINIMUM = 2**64 // -2
INT64_MAXIMUM = (2**64 // 2) - 1


def _get_float_maximum(exponent: int, mantissa: int) -> float:
    return float(
        2 ** (2 ** (exponent - 1) - 1) * (1 + (2**mantissa - 1) / 2**mantissa)
    )


def _get_float_minimum(exponent: int) -> float:
    return float(2 ** (2 - 2 ** (exponent - 1)))


FLOAT32_EXPONENT = 8
FLOAT32_MANTISSA = 23
FLOAT64_EXPONENT = 11
FLOAT64_MANTISSA = 52

float32_minimum = _get_float_minimum(FLOAT32_EXPONENT)
float32_maximum = _get_float_maximum(FLOAT32_EXPONENT, FLOAT32_MANTISSA)
float64_minimum = _get_float_minimum(FLOAT64_EXPONENT)
float64_maximum = _get_float_maximum(FLOAT64_EXPONENT, FLOAT64_MANTISSA)

# Metadata paths
TITLE_XPATH = [
    "gmd:identificationInfo",
    "gmd:MD_DataIdentification",
    "gmd:citation",
    "gmd:CI_Citation",
    "gmd:title",
    "gco:CharacterString",
    "/text()",
]

DATASET_LANGUAGE_XPATH = [
    "gmd:identificationInfo",
    "gmd:MD_DataIdentification",
    "gmd:language",
    "gmd:LanguageCode",
    "/text()",
]

TOPIC_CATEGORY_XPATH = [
    "gmd:identificationInfo",
    "gmd:MD_DataIdentification",
    "gmd:topicCategory",
    "gmd:MD_TopicCategoryCode",
    "/text()",
]

KEYWORD_XPATH = [
    "gmd:identificationInfo",
    "gmd:MD_DataIdentification",
    "gmd:descriptiveKeywords",
    "gmd:MD_Keywords",
    "gmd:keyword",
    "gco:CharacterString",
    "/text()",
]

QUALITY_SCOPE_XPATH = [
    "gmd:dataQualityInfo",
    "gmd:DQ_DataQuality",
    "gmd:scope",
    "gmd:DQ_Scope",
    "gmd:level",
    "gmd:MD_ScopeCode",
    "/text()",
]

SPATIAL_REPRESENTATION_TYPE_XPATH = [
    "gmd:identificationInfo",
    "gmd:MD_DataIdentification",
    "gmd:spatialRepresentationType",
    "gmd:MD_SpatialRepresentationTypeCode",
    "@codeListValue",
]

ABSTRACT_XPATH = [
    "gmd:identificationInfo",
    "gmd:MD_DataIdentification",
    "gmd:abstract",
    "gco:CharacterString",
    "/text()",
]

LINEAGE_XPATH = [
    "gmd:dataQualityInfo",
    "gmd:DQ_DataQuality",
    "gmd:lineage",
    "gmd:LI_Lineage",
    "gmd:statement",
    "gco:CharacterString",
    "/text()",
]

METADATA_DATE_XPATH = [
    [
        "gmd:dateStamp",
        "gco:Date",
        "/text()",
    ],
    [
        "gmd:dateStamp",
        "gco:DateTime",
        "/text()",
    ],
]

METADATA_LANGUAGE_XPATH = [
    "gmd:language",
    "gmd:LanguageCode",
    "/text()",
]

RESOURCE_TYPE_XPATH = [
    "gmd:hierarchyLevel",
    "gmd:MD_ScopeCode",
    "@codeListValue",
]

FILE_IDENTIFIER_XPATH = [
    "gmd:fileIdentifier",
    "gco:CharacterString",
    "/text()",
]
