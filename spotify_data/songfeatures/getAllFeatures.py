import spotipy
import json
import ast
import happybase
from spotipy.oauth2 import SpotifyClientCredentials

from variables import VUID, MACHINE, DEPENDENT_TABLE, SPOTIFY_FEATURES, FEATURES_COLF, FEATURES_COL, FINAL_FEATURES

connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
dep_table = connection.table(DEPENDENT_TABLE)
feat_table = connection.table(SPOTIFY_FEATURES)
final_table = connection.table(FINAL_FEATURES)

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
   
    if total > 2000:    #everything is in the DB already, don't put it in again

        data = json.loads(val.itervalues().next())
        new_data = {}
        new_data['title'] = data['title']
        new_data['artist'] = data['artist']
        results = sp.search(q=new_data['title'], limit=1)
        for k,v in results.items():
            for j in v['items']:
                new_data['uri'] = j['uri']
        b.put(key,{FEATURES_COLF + ':' + FEATURES_COL : json.dumps(new_data)})
        b.send()
        print total,key
    
    total += 1


b = final_table.batch()

cnt = 0
for k,v in final_table.scan():
    cnt += 1
print "FINAL: ",cnt

index = 0
for key, val in feat_table.scan():
    
    if index >= cnt:
    
        hund += 1

        data = json.loads(val.itervalues().next())
        if 'uri' not in data:
            print index,":Missing URI: ", key
        else:
            print index,key,data
            result = sp.audio_features([data['uri'][14:]])
        
            if (result[0] != None) and len(result) == 1:
                print index, "GOOD"
                final_data = {}
                final_data['title'] = data['title']
                final_data['artist'] = data['artist']
                final_data['energy'] = result[0]['energy']
                final_data['liveness'] = result[0]['liveness']
                final_data['tempo'] = result[0]['tempo']
                final_data['speechiness'] = result[0]['speechiness']
                final_data['duration'] = result[0]['duration_ms']
                final_data['danceability'] = result[0]['danceability']
                b.put(key,{FEATURES_COLF + ':' + FEATURES_COL : json.dumps(final_data)})
                b.send()
            else:
                print index,key,"BAD"

    index += 1

final_cnt = 0
for key, val in final_table.scan():
    final_cnt += 1

print "FINAL TABLE COUNT: ", final_cnt

