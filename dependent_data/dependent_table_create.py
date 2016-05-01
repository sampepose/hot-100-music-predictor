import happybase
import json
from collections import defaultdict
import editdistance
import re
from datetime import datetime


from variables import MACHINE, VUID, SPOTIFY_TABLE, BBRD_TABLE, DEPENDENT_TABLE, DEPENDENT_COLUMN_FAMILY, DEPENDENT_COLUMN

def convert(uni):
    return unicodedata.normalize('NFKD', uni).encode('ascii', 'ignore')

def index():
    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    spotify_table = connection.table(SPOTIFY_TABLE)
    bbrd_table = connection.table(BBRD_TABLE)
   
    both_count = 0
    song_artists = {}

    spotify_count = 0
    spotify_seen_songs = []
    for key, data in spotify_table.scan():
        key = re.sub(r'\(feat.*\)', '', key).strip().lower()
        if key not in spotify_seen_songs:
            spotify_count += 1
            spotify_seen_songs.append(key)
        data = json.loads(data.itervalues().next())
        if key not in song_artists:
            song_artists[key] = {'title': data['title'], 'artist': data['artist'], 'isHot': False}
    
    spotify_start_date = datetime.strptime('2014-01-01', '%Y-%m-%d')

    bbrd_artist_count_map = defaultdict(int)
    bbrd_count = 0
    bbrd_seen_songs = []
    for key, data in bbrd_table.scan():
        key = re.sub(r'\(feat.*\)', '', key).strip().lower()
        data = json.loads(data.itervalues().next())
        date = datetime.strptime(data['date'], '%Y-%m-%d')
        bbrd_artist_count_map[data['artist']] += 1
        if date < spotify_start_date:
            continue
        if key not in bbrd_seen_songs:
            bbrd_count += 1
            bbrd_seen_songs.append(key)
        if key not in song_artists:
            foundEditDistance = False
            for key2 in song_artists.keys():
                if len(key) > 5 and editdistance.eval(key,key2) <= 2:
                    bbrd_count -= 1
                    spotify_count -=1
                    both_count +=1
                    print "Edit distance found: " + key + "\t" + key2
                    song_artists[key2]['isHot'] = True
                    foundEditDistance = True
                    continue
            if not foundEditDistance:
                song_artists[key] = {'title': data['title'], 'artist': data['artist'], 'isHot': True}
        else:
            bbrd_count -= 1
            spotify_count -= 1
            both_count += 1
            song_artists[key]['isHot'] = True

    depTable = connection.table(DEPENDENT_TABLE)
    b = depTable.batch()

    for sa in song_artists:
        data = song_artists[sa]
        data['ArtistPopularity'] = max(0, bbrd_artist_count_map[data['artist']] - 1)
        key = DEPENDENT_COLUMN_FAMILY + ":" + DEPENDENT_COLUMN
        b.put(sa, {key: json.dumps(data)})
    b.send()

    print("Spotify count", spotify_count)
    print("BBRD count", bbrd_count)
    print("Overlap", both_count)
    print("Total count", len(song_artists))

if __name__ == '__main__':
    index()
