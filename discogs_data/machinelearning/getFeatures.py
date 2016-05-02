import happybase
import json
import numpy as np

from variables import DCOG_F_TABLE, DCOG_F_COLUMN_FAMILY, DCOG_F_COLUMN, MACHINE, VUID

def getDiscogsFeatures(keys):
    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    f_table = connection.table(DCOG_F_TABLE)
    
    features = [] 
    for key in keys:
        row = f_table.row(key[0])
        data = json.loads(row.itervalues().next())
        features.append(data)
        
    return np.array(features)


