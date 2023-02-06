"""conftest.py."""

from .fixtures.dataframe.call_method import (  # noqa: F401
    dataframe_other,
    expected_dataframe,
    expected_dataframe_filter,
    expected_dataframe_grouped,
    expected_dataframe_joined,
    expected_dataframe_limit,
    expected_dataframe_schema,
    expected_dataframe_select,
    expected_dataframewrapper_name,
    expected_schema_grouped,
    expected_schema_joined,
    expected_schema_select,
    schema_other,
    temp_path,
)
from .fixtures.dataframe.common import (  # noqa: F401
    expected_empty_metadata,
    spark_session,
)
from .fixtures.dataframe.pipeline import (  # noqa: F401
    new_data,
    expected_dag_source,
    expected_categorical_dataset,
    schema,
    small_boxes,
    output_schema,
    new_string_category_column,
    new_num_rows,
    new_geometry_column,
    new_category_lookup_column,
    make_dummy_vector_file,
    make_dummy_csv,
    spark_dataframe,
)
from .fixtures.dataframe.to_zarr import (  # noqa: F401
    bng_index_column,
    boolean_column,
    bounds_column,
    cell_size,
    data_for_rasterisation,
    dataframe_for_rasterisations,
    expected_float32_dataset,
    expected_float64_dataset,
    expected_int16_dataset,
    expected_int32_dataset,
    expected_int64_dataset,
    expected_mask_dataset,
    float32_column,
    float64_column,
    geometry_column,
    int16_column,
    int32_column,
    int64_column,
    make_expected_dataset_path,
    num_rows,
    out_shape,
    schema_for_rasterisation,
    string_category_column,
)
from .fixtures.fixtures_graph import expected_dag  # noqa: F401
from .fixtures.fixtures_metadata import (  # noqa: F401
    expected_metadata,
    expected_without_keywords,
    expected_without_title,
    local_file_path,
    remote_url,
)
from .fixtures.fixtures_raster import (  # noqa: F401
    larger_cell_size_same_shape,
    larger_cell_size_smaller_shape,
    same_cell_size_same_shape,
    same_cell_size_same_shape_dataset,
    same_cell_size_same_shape_dataset_with_nodata,
    same_cell_size_smaller_shape,
    smaller_cell_size_same_shape,
    smaller_cell_size_smaller_shape,
)
from .fixtures.fixtures_vector import (  # noqa: F401
    expected_category_lookups,
    expected_HL_array,
    expected_HM_array,
    expected_name,
    expected_schema,
    expected_vector_metadata,
    gpdf,
)
