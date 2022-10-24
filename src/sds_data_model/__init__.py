"""SDS Data Model.

The following usage examples assume a pyspark enabled environment.

Part 1: A breakdown of the process
==================================

Reading in vector data
----------------------

Read in a vector file as a Spark DataFrame using
`pyspark_vector_files.read_vector_files()`:

.. code-block:: python

    sdf = DataFrameWrapper.from_files(
        name = 'vector_name',
        data_path = '/path/to/files/'
    )

Filename pattern matching
^^^^^^^^^^^^^^^^^^^^^^^^^

Optionally, `kwargs` can be used to affect the behaviour of
`pyspark_vector_files.read_vector_files()`. For example, patterns and extensions can
be used to help identify the exact paths of input files:

.. code-block:: python

    sdf = DataFrameWrapper.from_files(
        name = 'vector_name',
        data_path = '/path/to/files/',
        read_file_kwargs = {'pattern': 'filename_pattern*',
        'suffix': '.ext'
        }
    )


Calling pyspark methods
-----------------------

The `DataFrameWrapper` class has a generic method named `call_method` which will allow
the user to call any(?) valid pyspark DataFrame method on the dataframe.

`filter`
^^^^^^^^

Rows can be filtered from a spark data frame using SQL-like expressions. For example to
filter attributes based on name matches in a single column:

.. code-block:: python

    sdf = DataFrameWrapper.from_files(
        name = 'vector_name',
        data_path = '/path/to/files/'
    ).call_method('filter', 'col == "val"')

Multiple queries can be executed with `or`:

.. code-block:: python

    sdf = DataFrameWrapper.from_files(
        name = 'vector_name',
        data_path = '/path/to/files/'
    ).call_method('filter', 'col == "vala" or col == "valb"')

Or, to identify rows where values contain a certain string, you can use the following:

.. code-block:: python

    sdf = DataFrameWrapper.from_files(
        name = 'vector_name',
        data_path = '/path/to/files/'
    ).call_method('filter', 'col LIKE "%vala%"')


`select`
^^^^^^^^

Columns can be selected as follows:

.. code-block:: python

    sdf = DataFrameWrapper.from_files(
        name = 'vector_name',
        data_path = '/path/to/files/'
    ).call_method('select', [col('cola'), col('colb')])

Note how the strings of the column names require wrapping in
`pyspark.sql.functions.col`.

`join`
^^^^^^

Both aspatial and spatial data can be joined to to the vector data once it is in the
`DataFrameWrapper` class.

Aspatial:

.. code-block:: python

    extra_data = spark.read.option("header",True) \
     .csv('/path/to/my_data.csv')

    sdf = DataFrameWrapper.from_files(
        name = 'vector_name',
        data_path = '/path/to/files/'
    ).call_method('join', extra_data, 'name', 'left')

Here, 'name' and 'left' are the the extra arguments passed to `join`s `on` and `how`
parameters. Given everything passed to `call_method` is positional, these need to be in
the correct order, with optional arguments skipped with `None` as appropriate.

Spatial:

.. code-block:: python

    sdf1 = DataFrameWrapper.from_files(
        name = 'vector_name',
        data_path = '/path/to/files/'

    sdf2 = DataFrameWrapper.from_files(
        name = 'vector_name',
        data_path = '/path/to/files/'
    ).call_method('join', sdf1.data, 'name', 'left')

Note how the `data` object from the DataFrameWrapper classed `sdf1` is selected within
the `join` method.

Chaining Methods
^^^^^^^^^^^^^^^^

The `call_method` method has been designed so that methods can be chained together.
For example, `select` then `filter` can be combined. The key thing to note here
is that the Spark DataFrame within the DataFrameWrapper is updated in-place. For
example:

.. code-block:: python

    sdf = DataFrameWrapper.from_files(
        name = 'vector_name',
        data_path = '/path/to/files/') \
        .call_method('select', [col('cola'), col('colb')]) \
        .call_method('filter', 'cola == "vala" or cola == "valb"')


Indexing
--------

Once vector files have been read in and transformations complete, the next step
is to index the data. This process adds two additional columns to the dataframe
stored within `data` within the DataFrameWrapper. `bng` contains the two
British National Grid grid letters relevant to that feature and `bounds` is a
tuple with British National Grid coordinates for the cell relating to the grid
letters. The indexing functions are called from the `bng_indexer` module.

The `index` function has several arguments, but only `resolution` requires user
input as the rest have defaults. Recommended resolution is `100_000`.

For example:

.. code-block:: python

    sdf.index(resolution = 100_000)

The above code would update the `sdf` object, providing the wrapped dataframe
with the additional columns.

Some of the other arguments allow the user to provide custom column names for the
geometry column as an input, and the two output columns:

.. code-block:: python

    sdf.index(resolution = 100_000,
         geometry_column_name = "custom_geom",
         index_column_name = "custom_index",
         bounds_column_name = "custom_bounds")

**TODO: how do variations in `how` work?**

Once the spatial index has been created, the DataFrameWrapper is ready for writing
to a zarr file.

Writing to zarr
---------------

The final stage of the data model pipeline is to rasterise data to a standard
10 m grid in British National Grid projection, and write the data to a zarr
file. This can be done with the `to.zarr()` method:

.. code-block:: python

    sdf.to_zarr(path = '/path/to/out_directory/',
           data_array_name = 'array_name')


As with the indexing function, custom column names can be provided if they have
been changed from the defaults:

.. code-block:: python

    sdf.to_zarr(path = '/path/to/out_directory/',
           data_array_name = 'array_name',
            index_column_name: str = "custom_index",
            geometry_column_name: str = "custom_geometry")


Part 2: A full workflow
=======================

The whole workflow can be pulled together like this:

.. code-block:: python

    DataFrameWrapper.from_files(
        name = 'vector_name',
        data_path = '/path/to/files/',
        data_path = '/path/to/files/'
    ).call_method('filter', 'cola == "vala" or cola == "valb"'
    ).index(resolution = 100_000
    ).to_zarr(path = '/path/to/out_directory/',
           data_array_name = 'array_name')

"""
