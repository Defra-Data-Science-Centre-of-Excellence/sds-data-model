import pandas as pd  
   
# Updates datatypes to smaller memory allocation, except dtype int8 which is not accepted by rasterio package, so converted to int16
def _update_datatypes(
    df 
):    
    #identify float and integer columns, which are treated separately to other dtypes
    fcols = df.select_dtypes('float').columns
    icols = df.select_dtypes('integer').columns

    df[fcols] = df[fcols].apply(pd.to_numeric, downcast = 'float')
    df[icols] = df[icols].apply(pd.to_numeric, downcast = 'integer')

    def convert_int_dtype(col):
        dtype = col.dtypes
        if dtype == "int8":
            return col.astype("int16")
        else:
            return col.astype(str(dtype))

    df[icols] = df[icols].apply(convert_int_dtype)
    
    #convert all remaining column types 
    df = df.convert_dtypes(convert_integer = False, convert_floating = False)
    
    return df
