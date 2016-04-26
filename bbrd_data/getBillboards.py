import happybase
import billboard
import json
from time import sleep

from variables import BBRD_TABLE, VUID, MACHINE, BBRD_COLUMN_FAMILY

target_chart = 'hot-100'

cur_chart = billboard.ChartData(target_chart)


connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
table = connection.table(BBRD_TABLE)
b = table.batch()

while(cur_chart.previousDate):
    print(cur_chart.date)
    for song in cur_chart:

        key = song.title.lower() + '_' + song.artist.lower()
        song_data = {'title':song.title,
                     'artist': song.artist,
                     'date' : cur_chart.date,
                     'weeks': song.weeks,
                     'peakPos' : song.peakPos,
                     'lastPos' : song.lastPos,
                     'rank' : song.rank,
                     'change' : song.change}

        b.put(key, {BBRD_COLUMN_FAMILY+':'+cur_chart.date: json.dumps(song_data)})

    b.send()                                                                                           
    sleep(2)
    cur_chart = billboard.ChartData(target_chart, cur_chart.previousDate)

