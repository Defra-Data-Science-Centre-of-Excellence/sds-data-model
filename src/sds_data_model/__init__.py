"""The following usage examples assume a pyspark enabled environment.

The `DataFrameWrapper` class
============================

The SDS data model DataFrameWrapper class is a thin wrapper around a
`Spark DataFrame`_.

It consists of:

* A name
* A Spark DataFrame instance
* An instance of the SDS data model :class:`sds_data_model.metadata.Metadata` class

Wrapping a Spark DataFrame in this way allows us to bind data and metadata together,
abstract common patterns to methods, and capture transformations.

Reading in data
---------------

To read a file, you just need to specify a name and a path:

.. code-block:: python

    sdf = DataFrameWrapper.from_files(
        name="vector_name",
        data_path="/path/to/files/",
    )

You can construct a DataFrameWrapper instance from several different spatial and
aspatial file formats using the
:meth:`sds_data_model.dataframe.DataFrameWrapper.from_files` alternative constructor.
`from_files` will use a different reader depending on the file extension.

* For Excel Workbook (`.xlsx`, `.xls`, `.xlsm`, and `.xlsb`) or OpenDocument Format
    (`.odf`, `.ods`, `.odt`) files, it will use `pyspark.pandas.read_excel`_.
* For CSV, it will use `pyspark.sql.DataFrameReader.csv`_.
* For JSON, it will use `pyspark.sql.DataFrameReader.json`_.
* For Parquet, it will use `pyspark.sql.DataFrameReader.parquet`_.
* For GeoPackage, it will use `pyspark_vector_files.gpkg.read_gpkg`_.

It will assume that any other file type is a vector file and will try and use
`pyspark_vector_files.read_vector_files`_ to read it.

.. warning::
    Databricks File System paths

    The Excel, OpenDocument, CSV, JSON, and Parquet readers use Spark API Format
    file paths, i.e. `dbfs:/path/to/file.ext`, whereas, the GeoPackage and other
    vector file type readers use the File API Format, i.e. `/dbfs/path/to/file.ext`.

Passing keyword arguments to the underlying reader
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The `read_file_kwargs` argument allows you to pass `kwargs` to the underlying reader.

For example, you can pass `kwargs` to `pyspark.pandas.read_excel`_ to read a specific
section of an OpenDocument Spreadsheet:

.. code-block:: python

    ods_df = DataFrameWrapper.from_files(
        name="opendocument spreadsheet example",
        data_path="dbfs:/path/to/file.ods",
        read_file_kwargs={
            "sheet_name": "1",
            "skiprows": 10,
            "header": [0, 1],
            "nrows": 23,
        },
    )

Or you can pass `kwargs` to `pyspark.sql.DataFrameReader.csv`_ to read a CSV with a
header:

.. code-block:: python

    csv_df = DataFrameWrapper.from_files(
        name="csv example",
        data_path="dbfs:/path/to/file.csv",
        read_file_kwargs={
            "header": True,
            "inferSchema": True,
        },
    )

Or you can pass `kwargs` to `pyspark_vector_files.gpkg.read_gpkg`_ to read a GeoPackage
without returning the GeoPackage Binary Header:

.. code-block:: python

    gpkg_df = DataFrameWrapper.from_files(
        name="geopackage example",
        data_path="/dbfs/path/to/file.gpkg",
        read_file_kwargs={
            "drop_gpb_header": True,
        },
    )

.. warning::
    GeoPackage JDBC dialect

    To read a GeoPackage using Spark's JDBC drivers, you will need to
    register a custom mapping of GeoPackage to Spark Catalyst types.

    See `register the geopackage dialect`_ for details.

Or you can pass `kwargs` to `pyspark_vector_files.read_vector_files` to read multiple
Shapefiles into single DataFrame:

.. code-block:: python

    vector_df = DataFrameWrapper.from_files(
        name="vector name",
        data_path="/dbfs/path/to/files",
        read_file_kwargs={
            "pattern": "filename_pattern*",
            "suffix": ".ext",
        },
    )


Calling pyspark methods
-----------------------

The `DataFrameWrapper` class has a generic method named `call_method` which will allow
the user to call `any valid Spark DataFrame method`_ on the underlying DataFrame
instance.

If the method returns a Spark DataFrame the underlying DataFrame instance
will be updated, if not, the output of the method will be returned.

`filter`
^^^^^^^^

The Spark DataFrame `filter`_ method allow you to select rows using SQL-like
expressions. You can call this method by passing the name of the method, followed by
the condition you want to filter on, to the `DataFrameWrapper`'s `call_method` method.

For example, to filter attributes based on name matches in a single column:

.. code-block:: python

    sdf = DataFrameWrapper.from_files(
        name="vector_name",
        data_path="/path/to/files/",
    ).call_method("filter", "col == 'val'")

Multiple queries can be executed with `or`:

.. code-block:: python

    sdf = DataFrameWrapper.from_files(
        name="vector_name",
        data_path="/path/to/files/",
    ).call_method(
        "filter",
        "col == 'val_a' or col == 'val_b'",
    )

Or, to identify rows where values contain a certain string, you can use the following:

.. code-block:: python

    sdf = DataFrameWrapper.from_files(
        name="vector_name",
        data_path="/path/to/files/",
    ).call_method(
        "filter",
        "col LIKE '%val_a%'",
    )


`select`
^^^^^^^^

The Spark DataFrame `select`_ method allow you to select columns by name. You can call
this method by passing the name of the method, followed by the columns you want to
select, to the `DataFrameWrapper`'s `call_method` method.

For example, columns can be selected as follows:

.. code-block:: python

    sdf = DataFrameWrapper.from_files(
        name="vector_name", data_path="/path/to/files/"
    ).call_method(
        "select",
        [col("col_a"), col("col_b")],
    )

.. warning::
    Unlike the underlying method, column names cannot be passed as strings.

    Instead, they must be wrapped in `pyspark.sql.functions.col`_.

`join`
^^^^^^

The Spark DataFrame `join`_ method allow you to join two Spark DataFrames. You can call
this method by passing the name of the method, followed by the relevant arguments, to
the `DataFrameWrapper`'s `call_method` method.

.. code-block:: python

    sdf1 = DataFrameWrapper.from_files(
        name="vector_name",
        data_path="/path/to/files/",
    )

    sdf2 = DataFrameWrapper.from_files(
        name="vector_name",
        data_path="/path/to/files/",
    ).call_method(
        "join",
        other=sdf1.data,
        on="name",
        how="left",
    )

Note how the `data` object from the DataFrameWrapper classed `sdf1` is selected within
the `join` method.

Chaining Methods
^^^^^^^^^^^^^^^^

The `call_method` method has been designed so that methods can be chained together.
For example, `select` then `filter` can be combined. The key thing to note here
is that the Spark DataFrame within the DataFrameWrapper is updated in-place. For
example:

.. code-block:: python

    sdf = (
        DataFrameWrapper.from_files(
            name="vector_name",
            data_path="/path/to/files/",
        )
        .call_method(
            "select",
            [col("col_a"), col("col_b")],
        )
        .call_method(
            "filter",
            "col_a == 'val_a' or col_a == 'val_b'",
        )
    )


Indexing
--------

Once vector files have been read in and transformations complete, the next step
is to index the data. This process adds two additional columns to the DataFrame
stored within `data` within the DataFrameWrapper. `bng_index` contains the two
British National Grid grid letters relevant to that feature and `bounds` is a
tuple with British National Grid coordinates for the cell relating to the grid
letters. The indexing functions are called from the `bng_indexer`_ library.

For example:

.. code-block:: python

    sdf.index()

The above code would update the `sdf` object, providing the wrapped DataFrame
with the additional columns.

Some of the other arguments allow the user to provide custom column names for
the geometry column as an input, and the two output columns:

.. code-block:: python

    sdf.index(
        resolution=10_000,
        geometry_column_name="custom_geom",
        index_column_name="custom_index",
        bounds_column_name="custom_bounds",
    )

For polygons/multipolygons 'how' methods of 'bounding box', 'intersects' and
'contains' are available. The 'bounding box' method returns all grid cells
covering the bounding box of the geometry; the 'intersects' method is the
default returning the grid cells that the polygon intersects; the 'contains'
methods additionally returns a boolean for each grid cell that the polygon
intersects with indicating whether that grid cell is contained by the polygon
(True) or intersects but is not contained (False).

Once the spatial index has been created, the DataFrameWrapper is ready for
writing to a zarr file.

Writing to zarr
---------------

The final stage of the data model pipeline is to rasterise data to a standard
10 m grid in British National Grid projection, and write the data to a zarr
file. This can be done with the `.to_zarr` method:

.. code-block:: python

    sdf.to_zarr(
        path="/path/to/out_directory/",
    )


As with the indexing function, custom column names can be provided if they have
been changed from the defaults:

.. code-block:: python

    sdf.to_zarr(
        path="/path/to/out_directory/",
        index_column_name="custom_index",
        geometry_column_name="custom_geometry",
    )

In line with the `xarray.Dataset.to_zarr` method, `to_zarr` will accept both a
path to a directory and a path to a directory suffixed with .zarr.

Overwriting
^^^^^^^^^^^

The `to_zarr` method has a default overwrite flag set to False. This means that
if a zarr dataset exists in the path provided to the method, then an error will
be thrown. The flag can be set by the user to True to allow overwriting.


A full workflow
---------------

The whole workflow can be pulled together like this:

.. code-block:: python

    DataFrameWrapper.from_files(
        name="vector_name",
        data_path="/dbfs/path/to/files",
        read_file_kwargs={
            "pattern": "filename_pattern*",
            "suffix": ".ext",
        },
    ).call_method("filter", "col_a == 'val_a' or col_a == 'val_b'").index().to_zarr(
        path="/path/to/out_directory/",
        data_array_name="array_name",
    )

The `DatasetWrapper` class
============================

The SDS data model DatasetWrapper class is a thin wrapper around a
`Xarray Dataset`_.

Reading in data
---------------

Input data that is not the shape of the British National Grid at 10m cell size
will be reshaped/placed into an array of that shape.
Data that does not have a cell size of 10m will be resampled to that cell size.

Read a file where that has a 10m cell size and British National Grid extent:

.. code-block:: python

    ds = DatasetWrapper.from_files(
        data_path="/path/to/file.ext",
    )


Read the first and fourth band from a file where both bands are categorical,
the nodata value/variable does not exist in the metadata,
the data does not have BNG extent,
and the data uses a large datatype e.g. float32:

.. code-block:: python

    ds = DatasetWrapper.from_files(
        data_path="path/to/file.ext",  # path to file
        bands=["0", "4"],  # select band names
        categorical=True,  # all input bands are categorical
        nodata=-9999,  # set a nodata value
        out_path="path/to/reshaped_file.zarr",  # provide a path for the BNG raster
        overwrite=True,  # overwrite the `out_path` if it exists
        chunks=300,  # use a small chunksize because of the large data type
    )


.. _`Spark DataFrame`:
    https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.html

.. _`pyspark.pandas.read_excel`:
    https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/api/pyspark.pandas.read_excel.html

.. _`pyspark.sql.DataFrameReader.csv`:
    https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrameReader.csv.html

.. _`pyspark.sql.DataFrameReader.json`:
    https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrameReader.json.html

.. _`pyspark.sql.DataFrameReader.parquet`:
    https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrameReader.parquet.html

.. _`pyspark_vector_files.gpkg.read_gpkg`:
    https://defra-data-science-centre-of-excellence.github.io/pyspark-vector-files/usage.html#read-a-layer-into-a-spark-dataframe

.. _`pyspark_vector_files.read_vector_files`:
    https://defra-data-science-centre-of-excellence.github.io/pyspark-vector-files/api.html

.. _`register the geopackage dialect`:
    https://defra-data-science-centre-of-excellence.github.io/pyspark-vector-files/usage.html#register-the-geopackage-dialect

.. _`any valid Spark DataFrame method`:
    https://spark.apache.org/docs/3.1.1/api/python/reference/pyspark.sql.html#dataframe-apis

.. _`filter`:
    https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.filter.html

.. _`select`:
    https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.select.html

.. _`pyspark.sql.functions.col`:
    https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.col.html

.. _`join`:
    https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.join.html

.. _`bng_indexer`:
    https://github.com/Defra-Data-Science-Centre-of-Excellence/bng-indexer

.. _`Xarray Dataset`:
    https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html
"""  # noqa: B950
