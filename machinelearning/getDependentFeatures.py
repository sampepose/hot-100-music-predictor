import happybase
import json
import numpy as np

from variables import MACHINE, VUID, DEPENDENT_TABLE

# http://stackoverflow.com/questions/9590382/forcing-python-json-module-to-work-with-ascii
def ascii_encode_dict(data):
    ascii_encode = lambda x: x.encode('ascii', 'ignore') if isinstance(x, unicode) else x
    return dict(map(ascii_encode, pair) for pair in data.items())

def getDependentFeatures():
    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    table = connection.table(DEPENDENT_TABLE)
    
    keys = []
    features = []
    labels = []
    for key, data in table.scan():
        data = json.loads(data.itervalues().next(), object_hook=ascii_encode_dict)
        keys.append([key, data['title'], data['artist']])
        features.append([data['ArtistPopularity']])
        labels.append(1 if data['isHot'] == True else 0)
    return np.array(keys), np.array(features), np.array(labels)
