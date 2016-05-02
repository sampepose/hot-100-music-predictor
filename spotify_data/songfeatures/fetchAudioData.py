import spotipy
import json
import ast
import happybase
from spotipy.oauth2 import SpotifyClientCredentials

from variables import VUID, MACHINE, DEPENDENT_TABLE, SPOTIFY_FEATURES, FEATURES_COLF, FEATURES_COL, FINAL_FEATURES

def getAudioData():

    print "Finding audio features..."

    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    dep_table = connection.table(DEPENDENT_TABLE)
    feat_table = connection.table(SPOTIFY_FEATURES)
    final_table = connection.table(FINAL_FEATURES)

    clientID = '78c78eb5db914f579c807115e26d40a7'
    clientSecret = '8be4d7a61a7d4fb79d3606ef785941c1'

    scc = SpotifyClientCredentials(clientID, clientSecret)
    sp = spotipy.Spotify(client_credentials_manager=scc)

    song_list = list()

    b = feat_table.batch()

# URIs finally populated, 1852 entries
    for key,val in dep_table.scan():
   
    #check if key is in feat_table
        if (len(feat_table.row(key))) == 0:
            print "Adding ",key," to SPOTIFY_FEATURES"
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
    # print "Done adding to FEATURES table..."

    b = final_table.batch()

    for key, val in feat_table.scan():
    
        data = json.loads(val.itervalues().next())
        #if 'uri' not in data:
            #print "Missing URI, can't search for: ", key
        #else:
        if 'uri' in data:
            if (len(final_table.row(key))) == 0:    
                result = sp.audio_features([data['uri'][14:]])
                if (result[0] != None) and len(result) == 1:
                    print "Adding to FINAL_FEATURES: ",key
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
                #else:
                    #print "Search couldn't find features for: ", key

    b.send()

    #final_cnt = 0
    #for key, val in final_table.scan():
        #final_cnt += 1

    # print "FINAL TABLE COUNT: ", final_cnt

