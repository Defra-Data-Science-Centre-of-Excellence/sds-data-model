import pandas as pd  
   

def _update_datatypes(
    df 
):    
    df = df.infer_objects()
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
    
    return df
