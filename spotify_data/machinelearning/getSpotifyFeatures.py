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

    #Features to keep running stats of:
    
    count = 0
    a_energy = 0
    a_liveness = 0
    a_tempo = 0
    a_speechiness = 0
    a_danceability = 0
    a_duration = 0

    spotify_features = []

    for key in keys:
        
        d = table1.row[key[0]]
        if len(d) == 0:
            d = table2.row[key[0]]
        
        data = json.loads(d.itervalues().next())

    #table = connection.table(TABLE_S_FIXED)

    #for key, d in table.scan():     #this is the table of all songs in spotify
        #data = json.loads(d.itervalues().next())
        count = count + 1
        energy = data['energy']
        live = data['liveness']
        tempo = data['tempo']
        speech = data['speechiness']
        dance = data['danceability']
        time = data['duration_ms']
        spotify_features.append([key,energy,live,tempo,speech,dance,time]) 
        a_energy += energy
        a_liveness += live
        a_tempo += tempo
        a_speechiness += speech
        a_danceability += dance
        a_duration += time


    #for key, d in table.scan():
        #data = json.loads(d.itervalues().next())
        #count = count + 1
        #energy = data['energy']
        #live = data['liveness']
        #tempo = data['tempo']
        #speech = data['speechiness']
        #dance = data['danceability']
        #time = data['duration_ms']
        #spotify_features.append([key,energy,live,tempo,speech,dance,time])
        #a_energy += energy
        #a_liveness += live
        #a_tempo += tempo
        #a_speechiness += speech
        #a_danceability += dance
        #a_duration += time

    print "Total count: ", count
    print "Average energy: ",(a_energy/count)
    print "Average liveness: ",(a_liveness/count)
    print "Average tempo: ",(a_tempo/count)
    print "Average speechiness: ",(a_speechiness/count)
    print "Average danceability: ",(a_danceability/count)
    print "Average duration: ",(a_duration/count)

    print spotify_features

    return np.array(spotify_features)

if __name__ == '__main__':
    getSpotifyFeatures()


