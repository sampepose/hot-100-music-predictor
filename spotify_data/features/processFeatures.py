import json
import happybase

from variables import MACHINE, VUID, TABLE_NAME_S, COLUMN_FAMILY_S, COLUMN_S, TABLE_NAME_BB, COLUMN_FAMILY_BB, COLUMN_BB

with open('new_billboard.json', 'w') as bb_new:
    with open('billboardout.json', 'r') as data_file:
        for line in data_file:
            if line[0:2] == "}{":
                bb_new.write('},\n')
                bb_new.write('{\n')
            else:
                bb_new.write(line)


with open('new_spotify.json', 'w') as s_new:
    with open('song_features.json') as data_file:
        for line in data_file:
            if line[0:2] == "}{":
                s_new.write('},\n')
                s_new.write('{\n')
            else:
                s_new.write(line)


with open('new_spotify.json', 'r') as sp:
    json_data = json.load(sp)

connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
table = connection.table(TABLE_NAME_S)

b = table.batch()

tracktable = connection.table('TrackID')
i = 0
for elem in json_data:
    if 'error' not in elem:
        data = {}
        for k,v in elem.items():
            data[k] = v
        # need to scan tracktable to get title, artist based on data['id']
        # find elem['id'] in tracktable
        for k,v in tracktable.scan():
            for k2, val in v.items():
                trackID = val[13:-2]
                if elem['id'] == trackID:
                    key = k
        #print(key, {COLUMN_FAMILY_S+ ":" + COLUMN_S: json.dumps(data)})
        b.put(key, {COLUMN_FAMILY_S + ":" + COLUMN_S: json.dumps(data)})
b.send()

#with open('new_spotify.json', 'r') as sp:
#    json_data = json.load(sp)


