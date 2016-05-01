import spotipy
import json
import ast
import happybase
from spotipy.oauth2 import SpotifyClientCredentials

from variables import VUID, MACHINE, DEPENDENT_TABLE, SPOTIFY_FEATURES, FEATURES_COLF, FEATURES_COL

connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
dep_table = connection.table(DEPENDENT_TABLE)

feat_table = connection.table(SPOTIFY_FEATURES)

clientID = '78c78eb5db914f579c807115e26d40a7'
clientSecret = '8be4d7a61a7d4fb79d3606ef785941c1'

scc = SpotifyClientCredentials(clientID, clientSecret)
sp = spotipy.Spotify(client_credentials_manager=scc)

song_list = list()

total = 0
found = 0

b = feat_table.batch()

# URIs finally populated, 1852 entries
for key,val in dep_table.scan():
    
    if total == 0:

        data = json.loads(val.itervalues().next())
        new_data = {}
        new_data['title'] = data['title']
        new_data['artist'] = data['artist']
        results = sp.search(q=new_data['title'], limit=1)
        for k,v in results.items():
            for j in v['items']:
                new_data['uri'] = j['uri']
                print j['uri']
        b.put(key,{FEATURES_COLF + ':' + FEATURES_COL : json.dumps(new_data)})
        b.send()

    total += 1

cnt = 0
for key, val in feat_table.scan():
    
    data = json.loads(val.itervalues().next())
    if 'uri' in data:
        # search on that uri
        result = sp.audio_features(data['uri'])
        print key,result
    
    else:
        print "URI not found: ",key

#cnt = 0
#uri_list = list()
#for i in song_list:
#    results = sp.search(q=i[1], limit=1)    
#    for k,v in results.items():
 #       for m in (v['items']):
#            uri = m['uri']
#            cnt += 1
#            print cnt
#            uri_list.append((i[0],i[1],i[2],uri))


