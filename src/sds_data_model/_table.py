from pandas import DataFrame, to_numeric

def _convert_int_dtype(col):
    """Converts dtype int8 which is not accepted by rasterio package, so converted to int16

    Example:
        >>> col = 

        >>> _convert_int_dtype(col)

    Args:
        col (_type_): Dataframe column 

    Returns:
        str(dtype): _description_
    """
    dtype = col.dtypes
    if dtype == "int8":
        return col.astype("int16")
    else:
        return col.astype(str(dtype))  


def _update_datatypes(
    df 
):    
    """Updates column datatypes to smaller memory allocation, apart from int8 types, which are converted to int16

    Example:
        >>> df = read_in_dataframe

        >>> updated_df = _update_datatypes(df)

        >>> print(updated_df.dtypes)


    Returns:
        _type_: _description_
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
