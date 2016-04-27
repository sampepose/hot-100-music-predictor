from __future__ import print_function
import spotipy
import subprocess
import happybase
import json
import sys
import csv
import os

# spotify = spotipy.Spotify()

from variables import MACHINE, VUID, PAGE_TABLE_SPOTIFY, COLUMN_FAMILY_SPOTIFY, COLUMN_SPOTIFY, PAGE_TABLE_SPOTIFY_TRACKS, TRACK_COLUMN_FAMILY, TRACK_COLUMN

clientID = '78c78eb5db914f579c807115e26d40a7'
clientSecret = '8be4d7a61a7d4fb79d3606ef785941c1'

connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
table = connection.table(PAGE_TABLE_SPOTIFY_TRACKS)

ids = list()

for k, v in table.scan():
    for key, val in v.items():
        # {"trackID": "1ds2QsfhAAfRiaFMGDzrdb"}
        trackID = val[13:-2]
        ids.append(trackID)

# returns info about the track based on ID
for track in ids:
    s_url = '"https://api.spotify.com/v1/audio-features/' + track + '"'
    cmd = 'curl -X GET ' + s_url + ' -H "Accept: application/json" -H "Authorization: Bearer BQDi0NQlIzm-lFqJ07fKcA4JnbAmadwCpTD23yiS88K1nSPeGAaYfLwqaxOY_wFjSFLn-6tCG_gywlSTdh-f_4K-y7Zd0HnQ9_5mh_m321VCksmXVnjdjT99WvGkGNwCoLIbxQ"'
    os.system(cmd)

# !!!!! Had to copy-paste output of all the os.system calls and place into a json file !!!!!
