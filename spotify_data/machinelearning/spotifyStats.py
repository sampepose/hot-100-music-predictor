import happybase
import sys
import json
import unicodedata
import re
from sklearn.feature_extraction import DictVectorizer
import numpy as np

from variables import MACHINE, VUID, FINAL_FEATURES

    
connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
table = connection.table(FINAL_FEATURES)

#Features to keep running stats of:
    
count = 0
a_energy = []
a_liveness = []
a_tempo = []
a_speechiness = []
a_danceability = []
a_duration = []

spotify_features = []

# Stats for ALL songs in dependent table that have features
for key, d in table.scan():

    data = json.loads(d.itervalues().next())
    count = count + 1
    a_energy.append(data['energy'])
    a_liveness.append(data['liveness'])
    a_tempo.append(data['tempo'])
    a_speechiness.append(data['speechiness'])
    a_danceability.append(data['danceability'])
    a_duration.append(data['duration'])

energy = np.array(a_energy)
live   = np.array(a_liveness)
tempo  = np.array(a_tempo)
speech = np.array(a_speechiness)
dance  = np.array(a_danceability)
time   = np.array(a_duration)

print "Total songs: ",count
print "\n\n"
print "\t\tAverage: \t\tStandard Deviation:"
print "Energy:\t\t", np.mean(energy),"\t\t",np.std(energy)
print "Liveness:\t",np.mean(live),"\t\t",np.std(live)
print "Tempo:\t\t",np.mean(tempo),"\t\t",np.std(tempo)
print "Speechiness\t",np.mean(speech),"\t",np.std(speech)
print "Danceability\t",np.mean(dance),"\t\t",np.std(dance)
print "Duration\t",np.mean(time),"\t\t",np.std(time)

#print "\n\n"
#print "Total count: ", count, 'songs'
#print "Average energy: ",reduce(lambda x, y: x + y, a_energy) / len(a_energy)
#print "Average liveness: ",reduce(lambda x, y: x + y, a_liveness) / len(a_liveness)
#print "Average tempo: ",reduce(lambda x, y: x + y, a_tempo) / len(a_tempo)
#print "Average speechiness: ",reduce(lambda x, y: x + y, a_speechiness) / len(a_speechiness)
#print "Average danceability: ",reduce(lambda x, y: x + y, a_danceability) / len(a_danceability)
#print "Average duration: ",reduce(lambda x, y: x + y, a_duration) / len(a_duration), 'ms'

