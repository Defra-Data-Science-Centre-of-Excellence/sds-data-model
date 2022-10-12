"""SDS Data Model.

The following usage examples assume a pyspark enabled environment.

Part 1: A breakdown of the process
==================================

Reading in vector data
----------------------

Read in a vector file as a Spark DataFrame using `pyspark_vector_files.read_vector_files()`:

.. code-block:: python

    sdf = DataFrameWrapper.from_files(
        name = 'vector_name',
        data_path = '/path/to/files/'
    )

Filename pattern matching
^^^^^^^^^^^^^^^^^^^^^^^^^
    
Optionally, `kwargs` can be used to affect the behaviour of `pyspark_vector_files.read_vector_files()`. 
For example, patterns and extensions can be used to help identify the exact paths of input files:

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

The `DataFrameWrapper` class has a generic method named `call_method` which will allow the user to 
call any(?) valid pyspark DataFrame method on the dataframe.

`filter`
^^^^^^^^

Rows can be filtered from a spark data frame using SQL-like expressions. For example to filter attributes
based on name matches in a single column:

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

Note how the strings of the column names require wrapping in `pyspark.sql.functions.col`.

`join`
^^^^^^

Both aspatial and spatial data can be joined to to the vector data once it is in the `DataFrameWrapper` class. 

Aspatial:

.. code-block:: python

    extra_data = spark.read.option("header",True) \
     .csv('/path/to/my_data.csv')
    
    sdf = DataFrameWrapper.from_files(
        name = 'vector_name',
        data_path = '/path/to/files/'
    ).call_method('join', extra_data, 'name', 'left')
    
Here, 'name' and 'left' are the the extra arguments passed to `join`s `on` and `how` parameters. Given everything #
passed to `call_method` is positional, these need to be in the correct order, with optional arguments skipped with 
`None` as appropriate. 

Spatial:

.. code-block:: python

    sdf1 = DataFrameWrapper.from_files(
        name = 'vector_name',
        data_path = '/path/to/files/'
    
    sdf2 = DataFrameWrapper.from_files(
        name = 'vector_name',
        data_path = '/path/to/files/'
    ).call_method('join', sdf1.data, 'name', 'left')
    
Note how the `data` object from the DataFrameWrapper classed `sdf1` is selected within the `join` method.



A full workflow
===============


"""