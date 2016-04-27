#Python script to add the CSV data into a DB

import happybase
import json
import sys
import csv
import re
import os

from variables import MACHINE, VUID, PAGE_TABLE_SPOTIFY, COLUMN_FAMILY_SPOTIFY, COLUMN_SPOTIFY, PAGE_TABLE_SPOTIFY_TRACKS, TRACK_COLUMN_FAMILY, TRACK_COLUMN

songs = dict()

urls = dict()

for i in os.listdir("./data/"):
    if (i.find('.csv') != -1):   #ignore non csv files
        fname = './data/' + i
        with open(fname) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                row['Date'] = i[0:10]
                
                songdata = (row['Date'],row['Position'], row['Streams'])
                #filter out: https://open.spotify.com/track/
                trackID = row['URL'][31:]
                title = row['Track Name']
                title = re.sub(r'\(feat.*\)', '', title).strip()
                songtitle = (title, row['Artist'])

                if songtitle in songs:
                    songs[songtitle].append(songdata)
                else:
                    a = list()
                    a.append(songdata)
                    songs[songtitle] = a

                if songtitle not in urls:
                    urls[songtitle] = trackID

connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
table = connection.table(PAGE_TABLE_SPOTIFY_TRACKS)
b = table.batch()
for k,v in urls.items():
    data = {}
    data['trackID'] = v
    key = k[0] + '_' + k[1]
    b.put(key, {TRACK_COLUMN_FAMILY + ":" + TRACK_COLUMN: json.dumps(data)})
b.send()

#add songs to hbase

connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
table = connection.table(PAGE_TABLE_SPOTIFY)
b = table.batch()

for k, v in songs.items():  #put them in the db
    for elem in v:
        #print elem
        data = {}
        data['song'] = k[0]
        data['artist'] = k[1]
        # data['date'] = elem[0]
        data['position'] = elem[1]
        data['streams'] = elem[2]
        key = k[0] + '_' + k[1]
        b.put(key, {COLUMN_FAMILY_SPOTIFY + ':' + elem[0]: json.dumps(data)})
    b.send()


