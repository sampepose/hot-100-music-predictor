#Python script to add the CSV data into a DB

import happybase
import json
import sys
import csv
import re
import os

from variables import MACHINE, VUID, SPOTIFY_TABLE, SPOTIFY_COLF, SPOTIFY_COL

connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
table = connection.table(SPOTIFY_TABLE)

b = table.batch()

for i in os.listdir("./spotify_data/data/"):
    if (i.find('.csv') != -1):   #ignore non csv files
        fname = './spotify_data/data/' + i
        with open(fname) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                
                data = {}
                data['title'] = row['Track Name'].lower()
                #title = re.sub(r'\(feat.*\)', '', title).strip()
                data['artist'] = row['Artist'].lower()
                key = data['title'] + '_' + data['artist']

                b.put(key, {SPOTIFY_COLF + ':' + SPOTIFY_COL : json.dumps(data)})
            
            b.send()
            print "All data put in happybase: ",SPOTIFY_TABLE

