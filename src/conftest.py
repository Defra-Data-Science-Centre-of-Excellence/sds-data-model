"""
conftest.py
"""
import pytest
from glob import glob

from sds_data_model.tests.fixtures.fixtures_DataFrameWrapper import *

from sds_data_model.tests.fixtures.fixtures_graph import *

from sds_data_model.tests.fixtures.fixtures_raster import *

from sds_data_model.tests.fixtures.fixtures_vector import *

from sds_data_model.tests.fixtures.fixtures_metadata import (
    expected_metadata,
    expected_without_keywords,
    expected_without_title,
    local_file_path,
    remote_url,
)