"""SDS Data Model.

The following usage examples are assuming a pyspark enabled environment. 

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

The `DataFrameWrapper` class has a generic method called `call_method` which will allow the user to 
call any(?) valid pyspark DataFrame method on the dataframe.

`filter`
^^^^^^^^


`select`
^^^^^^^^


A full workflow
===============


"""