import happybase
import sys
import json
import unicodedata
import re
from sklearn.feature_extraction import DictVectorizer
import numpy as np

from variables import MACHINE, VUID, SPOTIFY_COUNTS

def getDaysPlays(keys):
    
    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    table = connection.table(SPOTIFY_COUNTS)

    missing_keys = list()
    days_and_plays = []

    for key in keys:

        d = 0
        if (len(table.row(key[0]))) != 0:
            d = table.row(key[0])

        if (d != 0):
            data = json.loads(d.itervalues().next())
            plays = float(data['plays'])
            days = float(data['days'])
            days_and_plays.append([plays,days]) 
        else:
            days_and_plays.append([0,0])
            missing_keys.append(key[0])

    #if len(missing_keys) > 0:
    #    print "Spotify: Missing %d songs" % len(missing_keys)

    return np.array(days_and_plays)


