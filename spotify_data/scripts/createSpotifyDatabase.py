#Python script to add the CSV data into a DB
import unicodedata
import happybase
import json
import sys
import csv
import re
import os

from variables import MACHINE, VUID, SPOTIFY_TABLE, SPOTIFY_COLF, SPOTIFY_COL

def convert(uni):
    return unicodedata.normalize('NFKD', uni).encode('ascii', 'ignore')

connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
table = connection.table(SPOTIFY_TABLE)

b = table.batch()

songs = dict()

for i in os.listdir("./spotify_data/data/"):
    if (i.find('.csv') != -1):   #ignore non csv files
        fname = './spotify_data/data/' + i
        with open(fname) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
            
                data = {}
                data['title'] = convert(unicode(row['Track Name'], "utf-8")).lower()
                #title = re.sub(r'\(feat.*\)', '', title).strip()
                data['artist'] = convert(unicode(row['Artist'], "utf-8")).lower()
                key = data['title'] + '_' + data['artist']

                if key not in songs:
                    songs[key] = data
                
                b.put(key, {SPOTIFY_COLF + ':' + SPOTIFY_COL : json.dumps(data)})
            
b.send()
