import happybase
import sys

from variables import MACHINE, VUID, SPOTIFY_TABLE, SPOTIFY_COLF, SPOTIFY_COL


def setup():
    print "Creating..."
    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    connection.create_table(SPOTIFY_TABLE, {SPOTIFY_COLF: dict()})

def delete():
    print 'Deleting...'
    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    connection.delete_table(SPOTIFY_TABLE, True)

if __name__ == '__main__':
    
    
    if len(sys.argv) == 2:

        if sys.argv[1] == 'create':
            setup()
            print 'Done...'
        elif sys.argv[1] == 'delete':
            delete()
            print 'Done...'
        elif sys.argv[1] == 'scan':
            connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
            table = connection.table(SPOTIFY_TABLE)
            i = 0
            for k, v in table.scan():
                print i, k, v,
                i += 1
                if i > 10: break
            
        else:
            print "enter arg 'create, delete or scan'"
