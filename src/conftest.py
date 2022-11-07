"""conftest.py."""

from sds_data_model.tests.fixtures.fixtures_DataFrameWrapper import (  # noqa: F401
    dataframe_other,
    expected_attrs,
    expected_dataframe,
    expected_dataframe_filter,
    expected_dataframe_joined,
    expected_dataframe_limit,
    expected_dataframe_schema,
    expected_dataframe_select,
    expected_dataframewrapper_name,
    expected_empty_metadata,
    expected_hl_dataset,
    expected_schema_joined,
    expected_schema_select,
    hl_dataframe,
    hl_schema,
    hl_wrapper,
    hl_zarr_path,
    schema_other,
    spark_session,
    temp_path,
)
from sds_data_model.tests.fixtures.fixtures_graph import expected_dag  # noqa: F401
from sds_data_model.tests.fixtures.fixtures_metadata import (  # noqa: F401
    expected_metadata,
    expected_without_keywords,
    expected_without_title,
    local_file_path,
    remote_url,
)
from sds_data_model.tests.fixtures.fixtures_raster import (  # noqa: F401
    larger_cell_size_same_shape,
    larger_cell_size_smaller_shape,
    same_cell_size_same_shape,
    same_cell_size_same_shape_dataset,
    same_cell_size_same_shape_dataset_with_nodata,
    same_cell_size_smaller_shape,
    smaller_cell_size_same_shape,
    smaller_cell_size_smaller_shape,
)
from sds_data_model.tests.fixtures.fixtures_vector import (  # noqa: F401
    expected_category_lookups,
    expected_HL_array,
    expected_HM_array,
    expected_name,
    expected_schema,
    expected_vector_metadata,
    gpdf,
)
