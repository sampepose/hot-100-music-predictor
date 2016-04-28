# need to get the song features for those in the Billboard 100 list, but not in the Spotify list

import happybase
import spotipy
import os
from variables import VUID, MACHINE

connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)

#SPOTIFY TABLE
table = connection.table('SpotifyTable')
spotify_songs = list()
for k, v in table.scan():
    spotify_songs.append(k.lower())
    #spotify_songs.append((k[0:k.index('_')]).lower())

#DEPENDENT TABLE
table = connection.table('Dependent')
all_songs = list()
for k, v in table.scan():
    all_songs.append(k.lower())
    #all_songs.append((k[0:k.index('_')]).lower())

billboard_not_spotify = list()

for i in all_songs:
    if i not in spotify_songs:
        billboard_not_spotify.append(i)

billboard_only = dict()
for i in billboard_not_spotify:
    billboard_only[i[0:i.index('_')]] = i[i.index('_')+1:]
    #[Song]:[Artist]

#for k,v in billboard_only.items():
#    print 'Song:', k, 'Artist:', v

#NEED TO ACQUIRE trackID for all of these (song, artist) to then get features

sp = spotipy.Spotify()

url_list = list()

for k,v in billboard_only.items():
    results = sp.search(q=k, limit=1)
    for key,val in results.items():
        for m in (val['items']):
            url = m['external_urls']
            for a,b in url.items():
                url_list.append((k,v,b))

# 9 missing ones

# billboard_only ---- dict [song]:[artist]
# url_list       ---- list(song,artist,url)

#find the items in the dictionary that don't have a URL

#missing_url = list()
#have_url = list()

#for k,v in billboard_only.items():
#    found = False
#    for i in url_list:
#        if k == i[0]:
#            found = True
#    if (found == False):
#        missing_url.append((k,v))

#for i in missing_url:
#    print i
