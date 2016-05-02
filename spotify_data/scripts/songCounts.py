#Python script to add the CSV data into a DB
import unicodedata
import happybase
import json
import sys
import csv
import re
import os
import operator

from variables import MACHINE, VUID, SPOTIFY_COUNTS, SPOTIFY_COLF, SPOTIFY_COL

def convert(uni):
    return unicodedata.normalize('NFKD', uni).encode('ascii', 'ignore')

connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
table = connection.table(SPOTIFY_COUNTS)

b = table.batch()

song_days = dict()
song_plays = dict()

d = 0
for i in os.listdir("./spotify_data/data/"):
    if (i.find('.csv') != -1):   #ignore non csv files
        fname = './spotify_data/data/' + i
        with open(fname) as csvfile:
            print "Day: ",d
            d += 1
            reader = csv.DictReader(csvfile)
            for row in reader:
            
                data = {}
                data['title'] = convert(unicode(row['Track Name'], "utf-8")).lower()
                data['artist'] = convert(unicode(row['Artist'], "utf-8")).lower()
                key = data['title'] + '_' + data['artist']
                
                if (len(table.row(key))) == 0:
                    data['plays'] = float(row['Streams'])
                    data['days'] = 1
                else: #already in table, update values
                    olddata = json.loads(table.row(key).itervalues().next())
                    data['plays'] = float(row['Streams']) + float(olddata['plays'])
                    data['days'] = 1 + int(olddata['days'])
                
                b.put(key,{SPOTIFY_COLF + ':' + SPOTIFY_COL : json.dumps(data)})
            
            b.send()                    


