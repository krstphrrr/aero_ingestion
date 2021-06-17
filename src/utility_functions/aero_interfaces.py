import pandas as pd

# table structure in postgres

fields_dict = {
"ModelRunKey":pd.Series([],dtype='object'),
"Model":pd.Series([],dtype='object'),
"Coordinates":pd.Series([],dtype='object'),
"SurfaceSoilSource":pd.Series([],dtype='object'),
"MeteorologicalSource":pd.Series([],dtype='object'),
"ModelRunNotes":pd.Series([],dtype='object'),

}

type_translate = {
    np.dtype('int64'):'int',
    'Int64':'int',
    np.dtype("object"):'text',
    np.dtype('datetime64[ns]'):'timestamp',
    np.dtype('bool'):'boolean',
    np.dtype('float64'):'float(5)',
    }

aero_translate = {
    'int64':'int',
    'Int64':'int',
    "object":'text',
    'datetime64[ns]':'timestamp',
    'bool':'boolean',
    'float64':'float'
}
