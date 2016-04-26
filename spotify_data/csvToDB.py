#Python script to add the CSV data into a DB

import happybase
import json
import sys
import csv
import os

from variables import MACHINE, VUID, PAGE_TABLE, INDEX_TABLE, COLUMN_FAMILY, COLUMN

songs = dict()

for i in os.listdir("./SpotifyData/"):
    if (i.find('.csv') != -1):   #ignore non csv files
        fname = './SpotifyData/' + i
        with open(fname) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                row['Date'] = i[0:10]
                
                songdata = (row['Date'],row['Position'], row['Streams'])

                songtitle = (row['Track Name'], row['Artist'])

                if songtitle in songs:
                    songs[songtitle].append(songdata)
                else:
                    a = list()
                    a.append(songdata)
                    songs[songtitle] = a

#add songs to hbase

connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
table = connection.table(PAGE_TABLE)
b = table.batch()

for k, v in songs.items():  #put them in the db
    #k[0] = song
    #k[1] = artist
    #v[0] = date
    #v[1] = position
    #v[2] = num_stream
    #print k[0]
    for elem in v:
        #print elem
        data = {}
        data['date'] = elem[0]
        data['position'] = elem[1]
        data['streams'] = elem[2]
        data['artist'] = k[1]
        b.put(k[0], {COLUMN_FAMILY + ':' + k[1]: json.dumps(data)})
    b.send()


