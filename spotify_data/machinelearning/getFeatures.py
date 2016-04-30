import happybase
import sys
import json

from variables import MACHINE, VUID,PAGE_TABLE_SPOTIFY,COLUMN_FAMILY_SPOTIFY,COLUMN_SPOTIFY,PAGE_TABLE_SPOTIFY_TRACKS,TRACK_COLUMN_FAMILY,TRACK_COLUMN,BBRD_TABLE,BBRD_COLUMN_FAMILY,BBRD_COLUMN,DEPENDENT_TABLE,DEPENDENT_COLUMN_FAMILY,DEPENDENT_COLUMN,TABLE_NAME_S,COLUMN_FAMILY_S,COLUMN_S,TABLE_NAME_BB,COLUMN_FAMILY_BB,COLUMN_BB,TWITTER_TABLE,TWITTER_COLUMN_FAMILY,TWITTER_ARTIST_COLUMN,TWITTER_TITLE_COLUMN



def run():

    table = connection.table(TABLE_NAME_S)

#face the sun_Miguel {'song:data': '{"key": 5, "analysis_url":"http://echonest-analysis.s3.amazonaws.com/TR/enPigwcm-_KX8EnamOzKvbG9t6sVuo3U40LwHq5z8GGbhVw2csVlmMreJoIb8k-Zle0eW1d-KMuhtOTW8=/3/full.json?AWSAccessKeyId=AKIAJRDFEY23UEVW42BQ&Expires=1461801150&Signature=MlXzY5omLHN8oaHcqox3%2B8ksM74%3D","energy": 0.806, "liveness": 0.143, "tempo": 130.932, "speechiness": 0.0452, "uri":"spotify:track:0rUAPRZSwifI4HY8dMqJ40", "acousticness": 0.243, "danceability": 0.424, "track_href":"https://api.spotify.com/v1/tracks/0rUAPRZSwifI4HY8dMqJ40", "time_signature": 4, "duration_ms": 272773, "loudness":-4.585, "mode": 1, "valence": 0.182, "type": "audio_features", "id": "0rUAPRZSwifI4HY8dMqJ40", "instrumentalness": 0}'} 

#Features to keep track of:

    t2 = connection.table('SpotifyTraits')

    count = 0
    energy = 0
    liveness = 0
    tempo = 0
    speechiness = 0
    danceability = 0
    duration = 0

    for k, d in table.scan():

        data = json.loads(d.itervalues().next())
        count = count + 1
        energy = count + data['energy']
        liveness = liveness + data['liveness']
        tempo = tempo + data['tempo']
        speechiness = speechiness + data['speechiness']
        danceability = danceability + data['danceability']
        duration = duration + data['duration_ms']

    print "Total count: ", count
    print "Average energy: ",(energy/count)
    print "Average liveness: ",(liveness/count)
    print "Average tempo: ",(tempo/count)
    print "Average speechiness: ",(speechiness/count)
    print "Average danceability: ",(danceability/count)
    print "Average duration: ",(duration/count)


if __name__ == '__main__':
    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    if len(sys.argv) > 1:
        if sys.argv[1] == 'create':
            connection.create_table('SpotifyTraits', {'data' : dict()})
            run()

        elif sys.argv[1] == 'delete':
            connection.delete_table('SpotifyTraits', True)


    else:
        run()






