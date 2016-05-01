import happybase
import sys
import json
import unicodedata
import re
from sklearn.feature_extraction import DictVectorizer
import numpy as np

from variables import MACHINE, VUID, FINAL_FEATURES

def getSpotifyFeatures(keys):
    
    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    features = connection.table(FINAL_FEATURES)

    missing_keys = list()
    spotify_features = []

    for key in keys:

        d = 0
        if (len(features.row(key[0]))) != 0:
            d = features.row(key[0])

        if (d != 0):
            data = json.loads(d.itervalues().next())
            energy = data['energy']
            live = data['liveness']
            tempo = data['tempo']
            # speech = data['speechiness']
            dance = data['danceability']
            time = data['duration']
            spotify_features.append([energy,live,tempo,dance,time])
            # spotify_features.append([energy,live,tempo,speech,dance,time]) 
        else:
            spotify_features.append([0,0,0,0,0])
            missing_keys.append(key[0])

    if len(missing_keys) > 0:
        print "Spotify: Missing %d songs" % len(missing_keys)
    #    print "Couldn't find spotify features for: ",missing_keys

    return np.array(spotify_features)


