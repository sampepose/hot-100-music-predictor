import happybase
import json
import numpy as np

from makeFeatures_2 import makeFeatures_2
from variables import DCOG_F2_TABLE, DCOG_F2_COLUMN_FAMILY, DCOG_F2_COLUMN, MACHINE, VUID

def getDiscogsFeatures_2(keys):
    makeFeatures_2()
    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    f_table = connection.table(DCOG_F2_TABLE)
    
    features = [] 
    for key in keys:
        row = f_table.row(key[0])
        data = json.loads(row.itervalues().next())
        features.append(data)
        
    return np.array(features)


