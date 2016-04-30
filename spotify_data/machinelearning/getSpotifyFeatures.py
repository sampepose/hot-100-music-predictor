import happybase
import sys
import json
import unicodedata
import re
from sklearn.feature_extraction import DictVectorizer
import numpy as np

from variables import MACHINE, VUID, TABLE_S_FIXED, TABLE_NAME_BB

def getSpotifyFeatures(keys):
    
    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    table1 = connection.table(TABLE_S_FIXED)
    table2 = connection.table(TABLE_NAME_BB)

    missing_keys = list()
    spotify_features = []

    for key in keys:

        if (len(table1.row(key[0])) == 0):
            if (len(table2.row(key[0])) == 0):
                missing_keys.append(key[0])
                spotify_features.append([0,0,0,0,0,0])
                d = 0
            else:
                d = table2.row(key[0])
        else:
            d = table1.row(key[0])

        if (d != 0):
            data = json.loads(d.itervalues().next())
            count = count + 1
            energy = data['energy']
            live = data['liveness']
            tempo = data['tempo']
            speech = data['speechiness']
            dance = data['danceability']
            time = data['duration_ms']
            spotify_features.append([energy,live,tempo,speech,dance,time]) 

    #if len(missing_keys) > 0:
    #    print "Couldn't find spotify features for: ",missing_keys

    return np.array(spotify_features)


