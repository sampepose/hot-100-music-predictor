import happybase
import billboard
import json
import unicodedata

from time import sleep

from variables import BBRD_TABLE, VUID, MACHINE, BBRD_COLUMN_FAMILY

target_chart = 'hot-100'

cur_chart = billboard.ChartData(target_chart, "2003-11-29")

connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
table = connection.table(BBRD_TABLE)
b = table.batch()

def convert(uni):
    return unicodedata.normalize('NFKD', uni).encode('ascii', 'ignore')

while(cur_chart.previousDate):
    print(cur_chart.date)
    for song in cur_chart:
        title = convert(song.title)
        artist = convert(song.artist)
        date = convert(cur_chart.date)
        weeks = song.weeks
        peakPos = song.peakPos
        lastPos = song.lastPos
        rank = song.rank
        change = song.change
    
        key = title.lower() + '_' + artist.lower()
        song_data = {'title': title,
                     'artist': artist,
                     'date' : date,
                     'weeks': weeks,
                     'peakPos' : peakPos,
                     'lastPos' : lastPos,
                     'rank' : rank,
                     'change' : change}

        b.put(key, {BBRD_COLUMN_FAMILY+':'+cur_chart.date: json.dumps(song_data)})

    b.send()                                                                                           
    sleep(2)
    cur_chart = billboard.ChartData(target_chart, cur_chart.previousDate)
    
