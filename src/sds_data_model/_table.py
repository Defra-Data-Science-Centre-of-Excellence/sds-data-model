from pandas import DataFrame, Series, to_numeric

def _convert_int_dtype(
    col: Series
    ):
    """Converts dtype int8 to int16, as int8 is not accepted by rasterio package

    Example:
        >>> from pprint import pprint

        >>> from pandas import DataFrame, to_numeric, read_csv

        >>> df = read_csv( "s3://s3-ranch-019/30by30/lpis_ceh_lookup.csv")

        >>> icols = df.select_dtypes('integer').columns

        >>> df.loc[:, icols] = df.loc[:, icols].apply(to_numeric, downcast = 'integer')
    
        >>> pprint(df.dtypes)
        
        >>> df.loc[:, icols] =df.loc[:, icols].apply(_convert_int_dtype)

        >>> pprint(df.dtypes)

        >>> df.dtypes != 'int8'

    Args:
        col (Series): Data frame column to be converted

    Returns:
        str(dtype): string of column dtype
    """
    dtype = col.dtypes
    if dtype == "int8":
        return col.astype("int16")
    else:
        return col.astype(str(dtype))  


def _update_datatypes(
    df : DataFrame
):    
    """Updates column datatypes to smaller memory allocation, apart from int8 types, which are converted to int16

    Example:

        >>> from pprint import pprint

        >>> from pandas import DataFrame, to_numeric, read_csv

        >>> df = read_csv( "s3://s3-ranch-019/30by30/lpis_ceh_lookup.csv")

        >>> pprint(df.dtypes)

        >>> updated_df = _update_datatypes(df)

        >>> pprint(updated_df.dtypes)

    Args:
           df (DataFrame): Dataframe to be updated

    Returns:
            DataFrame: Dataframe with column dtypes updated 
    """
    #identify float and integer columns, which are treated separately to other dtypes
    fcols = df.select_dtypes('float').columns
    icols = df.select_dtypes('integer').columns

    df.loc[:, fcols] = df.loc[:, fcols].apply(to_numeric, downcast = 'float')
    df.loc[:, icols] = df.loc[:, icols].apply(to_numeric, downcast = 'integer')
    df.loc[:, icols] = df.loc[:, icols].apply(_convert_int_dtype)   

    #convert all remaining column types 
    df = df.convert_dtypes(convert_integer = False, convert_floating = False)
    
    return df
