import happybase
import sys


from variables import MACHINE, VUID, PAGE_TABLE_SPOTIFY, COLUMN_FAMILY_SPOTIFY, COLUMN_SPOTIFY, PAGE_TABLE_SPOTIFY_TRACKS, TRACK_COLUMN_FAMILY, TRACK_COLUMN

def setup():
    print "Creating..."
    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    connection.create_table(PAGE_TABLE_SPOTIFY, {COLUMN_FAMILY_SPOTIFY: dict()})
    connection.create_table(PAGE_TABLE_SPOTIFY_TRACKS, {TRACK_COLUMN_FAMILY: dict()})

def delete():
    print 'Deleting...'
    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    connection.delete_table(PAGE_TABLE_SPOTIFY, True)
    connection.delete_table(PAGE_TABLE_SPOTIFY_TRACKS, True)

if __name__ == '__main__':
    if len(sys.argv) == 2:
        if sys.argv[1] == 'delete':
            delete()
        elif sys.argv[1] == 'scan':
            connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
            table = connection.table(PAGE_TABLE_SPOTIFY_TRACKS)
            i = 0
            for k, v in table.scan():
                print i, k, v,
                i += 1
                if i > 10: break
            
            print "\n"
            print "\n"
            print "\n"

            table = connection.table(PAGE_TABLE_SPOTIFY)
            i = 0
            for k, v in table.scan():
                print i, k, v, 
                i += 1
                if i > 10: break

    else:
        setup()

    print 'Done...'




