import happybase
import sys
import json

from variables import MACHINE, VUID, SPOTIFY_FEATURES, FEATURES_COLF, FINAL_FEATURES 
def setup():
    print 'Creating...'
    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    connection.create_table(FINAL_FEATURES, {FEATURES_COLF: dict()})


def delete():
    print 'Deleting...'
    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    connection.delete_table(FINAL_FEATURES, True)


if __name__ == '__main__':
    if len(sys.argv) == 2:
        if sys.argv[1] == 'create':
            setup()
        elif sys.argv[1] == 'delete':
            delete()
        elif sys.argv[1] == 'scan':
            connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
            table = connection.table(FINAL_FEATURES)
            i = 0
            for k, v in table.scan():
                data = json.loads(v.itervalues().next())
                if 'energy' not in data:
                    print "MISSING: ",k
                i += 1    
            print "COUNT: ",i

    else:
        print 'Not enough params'
    print 'Done...'
