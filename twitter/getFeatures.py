import happybase
import json
import numpy as np

from variables import MACHINE, VUID, TWITTER_TABLE

# http://stackoverflow.com/questions/9590382/forcing-python-json-module-to-work-with-ascii
def ascii_encode_dict(data):
    ascii_encode = lambda x: x.encode('ascii', 'ignore') if isinstance(x, unicode) else x 
    return dict(map(ascii_encode, pair) for pair in data.items())

# Expects a 'key' matrix of [[key, title, artist], ...]
def getTwitterBaseline(keys):
    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    table = connection.table(TWITTER_TABLE)

    features = []
    for key in keys:
        title_row = table.row(key[1].lower().strip())
        artist_row = table.row(key[2].lower().strip())
    
        title_count = 0
        artist_count = 0
        if title_row:
            title_data = json.loads(title_row.itervalues().next(), object_hook=ascii_encode_dict)
            title_count = title_data['count']
        if artist_row:
            artist_data = json.loads(artist_row.itervalues().next(), object_hook=ascii_encode_dict)
            artist_count = artist_data['count']
        features.append([title_count, artist_count])
    return np.array(features)

def getTwitterSpecial(keys):
    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    table = connection.table(TWITTER_TABLE)

    features = []
    for key in keys:
        title_row = table.row(key[1].lower().strip())
        artist_row = table.row(key[2].lower().strip())

        title_count = 0
        artist_count = 0
        if title_row:
            title_data = json.loads(title_row.itervalues().next(), object_hook=ascii_encode_dict)
            title_count = title_data['weightedcount']
        if artist_row:
            artist_data = json.loads(artist_row.itervalues().next(), object_hook=ascii_encode_dict)
            artist_count = artist_data['weightedcount']
        features.append([title_count, artist_count])
    return np.array(features)
