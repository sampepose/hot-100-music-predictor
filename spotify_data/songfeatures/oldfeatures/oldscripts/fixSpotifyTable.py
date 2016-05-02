import json
import happybase
from variables import VUID, MACHINE, TABLE_NAME_S, TABLE_S_FIXED, COLUMN_FAMILY_S, COLUMN_S

connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
table = connection.table(TABLE_NAME_S)

new_table = connection.table(TABLE_S_FIXED)
b = new_table.batch()

for key, d in table.scan():

    data = json.loads(d.itervalues().next())
    b.put(key.lower(), {COLUMN_FAMILY_S + ":" + COLUMN_S: json.dumps(data)})

b.send()

