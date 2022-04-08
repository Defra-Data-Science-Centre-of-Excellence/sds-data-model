from itertools import product
from typing import Tuple

BNG = "OSGB36 / British National Grid"
BNG_XMIN = 0
BNG_YMIN = 0
BNG_XMAX = 700_000
BNG_YMAX = 1_300_000
BOX_SIZE = 100_000
CELL_SIZE = 10
TILE_SIZE = BOX_SIZE // CELL_SIZE
OUT_SHAPE = (TILE_SIZE, TILE_SIZE)


GMD = "{http://www.isotc211.org/2005/gmd}"
GML = "{http://www.opengis.net/gml/3.2}"
GMX = "{http://www.isotc211.org/2005/gmx}"
GCO = "{http://www.isotc211.org/2005/gco}"
CHARACTER_STRING = GCO + "CharacterString"

BoundingBox = Tuple[int, int, int, int]


def _get_bboxes(
    xmin: int = BNG_XMIN,
    ymin: int = BNG_YMIN,
    xmax: int = BNG_XMAX,
    ymax: int = BNG_YMAX,
    box_size: int = BOX_SIZE,
) -> Tuple[BoundingBox]:
    eastings = range(xmin, xmax, box_size)
    northings = range(ymax, ymin, -box_size)

    top_left_coordinates = product(northings, eastings)

    return tuple((x, y - box_size, x + box_size, y) for y, x in top_left_coordinates)


BBOXES = _get_bboxes()
