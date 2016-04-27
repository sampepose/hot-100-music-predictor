import happybase
import json
from datetime import datetime

from variables import MACHINE, VUID, PAGE_TABLE_SPOTIFY, BBRD_TABLE, DEPENDENT_TABLE, DEPENDENT_COLUMN_FAMILY, DEPENDENT_COLUMN

def hbaseAccelerationParser(table): #create UDF to format data
    finalTable = []
    for key, data in table.scan(): #don't need the key in my case
        print(key)
        print("\n")
        finalTable.append(data)
    return finalTable

def isLabel(tag):
    if tag.previous_element is not None and tag.previous_element.previous_sibling is not None:
        th = tag.previous_element.previous_sibling
        a = th.next_element
        return a['title'] == 'Record label'
    return False

def index():
    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    spotify_table = connection.table(PAGE_TABLE_SPOTIFY)
    bbrd_table = connection.table(BBRD_TABLE)
   
    song_artists = {}

    spotify_count = 0
    spotify_seen_songs = []
    for key, data in spotify_table.scan():
        key = key.lower()
        if key not in spotify_seen_songs:
            spotify_count += 1
            spotify_seen_songs.append(key)
        data = json.loads(data.itervalues().next())
        if key not in song_artists:
            song_artists[key] = {'title': data['song'], 'artist': data['artist'], 'isHot': False}

    spotify_start_date = datetime.strptime('2015-04-25', '%Y-%m-%d')

    bbrd_count = 0
    bbrd_seen_songs = []
    for key, data in bbrd_table.scan():
        key = key.lower()
        data = json.loads(data.itervalues().next())
        date = datetime.strptime(data['date'], '%Y-%m-%d')
        if date < spotify_start_date:
            continue
        if key not in bbrd_seen_songs:
            bbrd_count += 1
            bbrd_seen_songs.append(key)
        if key not in song_artists:
            song_artists[key] = {'title': data['title'], 'artist': data['artist'], 'isHot': True}
        else:
            song_artists[key]['isHot'] = True

    depTable = connection.table(DEPENDENT_TABLE)
    b = depTable.batch()

    for sa in song_artists:
        data = song_artists[sa]
        key = DEPENDENT_COLUMN_FAMILY + ":" + DEPENDENT_COLUMN
        b.put(sa, {key: json.dumps(data)})
    b.send()

    print("Spotify count", spotify_count)
    print("BBRD count", bbrd_count)
    print("Total count", len(song_artists))
    print("Overlap", (bbrd_count + spotify_count) - len(song_artists))



if __name__ == '__main__':
    index()
