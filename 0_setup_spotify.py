import happybase
import sys
import json

from variables import MACHINE, VUID, SPOTIFY_COUNTS, SPOTIFY_COLF, SPOTIFY_FEATURES, FEATURES_COLF, FINAL_FEATURES 
def setup():
    print 'Creating...'
    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    connection.create_table(SPOTIFY_COUNTS, {SPOTIFY_COLF: dict()})


def delete():
    print 'Deleting...'
    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    connection.delete_table(SPOTIFY_COUNTS, True)


if __name__ == '__main__':
    if len(sys.argv) == 2:
        if sys.argv[1] == 'create':
            setup()
        elif sys.argv[1] == 'delete':
            delete()
        elif sys.argv[1] == 'scan':
            connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
            table = connection.table(SPOTIFY_COUNTS)
            i = 0
            for k, v in table.scan():
                print k,v
                data = json.loads(v.itervalues().next())

    else:
        print 'Not enough params'
    print 'Done...'
