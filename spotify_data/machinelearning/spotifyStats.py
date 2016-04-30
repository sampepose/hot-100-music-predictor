import happybase
import sys
import json
import unicodedata
import re
from sklearn.feature_extraction import DictVectorizer
import numpy as np

from variables import MACHINE, VUID, TABLE_NAME_S


def getSpotifyStats():
    
    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    table = connection.table(TABLE_NAME_S)

    #Features to keep running stats of:
    
    count = 0
    a_energy = []
    a_liveness = []
    a_tempo = []
    a_speechiness = []
    a_danceability = []
    a_duration = []

    spotify_features = []

    for key, d in table.scan():

        data = json.loads(d.itervalues().next())
        
        count = count + 1
        energy = data['energy']
        live = data['liveness']
        tempo = data['tempo']
        speech = data['speechiness']
        dance = data['danceability']
        time = data['duration_ms']

        spotify_features.append([key,energy,live,tempo,speech,dance,time]) 

        a_energy.append(energy)
        a_liveness.append(live)
        a_tempo.append(tempo)
        a_speechiness.append(speech)
        a_danceability.append(dance)
        a_duration.append(time)

    print "Total count: ", count
    print "Average energy: ",reduce(lambda x, y: x + y, a_energy) / len(a_energy)
    print "Average liveness: ",reduce(lambda x, y: x + y, a_liveness) / len(a_liveness)
    print "Average tempo: ",reduce(lambda x, y: x + y, a_tempo) / len(a_tempo)
    print "Average speechiness: ",reduce(lambda x, y: x + y, a_speechiness) / len(a_speechiness)
    print "Average danceability: ",reduce(lambda x, y: x + y, a_danceability) / len(a_danceability)
    print "Average duration: ",reduce(lambda x, y: x + y, a_duration) / len(a_duration)

    


if __name__ == '__main__':
    getSpotifyFeatures()


