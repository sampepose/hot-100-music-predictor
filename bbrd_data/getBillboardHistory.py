import billboard
import json

from time import sleep

start_date = '2015-04-25'
end_date = '2016-04-23'

target_chart = 'hot-100'

all_bboard = {}

cur_chart = billboard.ChartData(target_chart)

while(cur_chart.previousDate and cur_chart.previousDate != start_date):
    
    for song in cur_chart:
        
        key = song.title + '_' + song.artist

        if key not in all_bboard:
            all_bboard[key] = {'title':song.title, 
                               'artist': song.artist,
                               'weeks': song.weeks,
                               'start_date' : 'Before'}
        if song.weeks == 1:
            all_bboard[key]['start_date'] = cur_chart.date
        
    sleep(3)
    cur_chart = billboard.ChartData(target_chart, cur_chart.previousDate)

print(json.dumps(all_bboard))
