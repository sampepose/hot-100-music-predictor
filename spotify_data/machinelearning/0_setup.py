import happybase
import sys


from variables import MACHINE, VUID, PAGE_TABLE_SPOTIFY,COLUMN_FAMILY_SPOTIFY,COLUMN_SPOTIFY,PAGE_TABLE_SPOTIFY_TRACKS,TRACK_COLUMN_FAMILY,TRACK_COLUMN,BBRD_TABLE,BBRD_COLUMN_FAMILY,BBRD_COLUMN,DEPENDENT_TABLE,DEPENDENT_COLUMN_FAMILY,DEPENDENT_COLUMN,TABLE_NAME_S,COLUMN_FAMILY_S,COLUMN_S,TABLE_NAME_BB,COLUMN_FAMILY_BB,COLUMN_BB,TWITTER_TABLE,TWITTER_COLUMN_FAMILY,TWITTER_ARTIST_COLUMN, TWITTER_TITLE_COLUMN

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
        if sys.argv[1] == 'create_databases':
            setup() #shouldn't be called from here though
        elif sys.argv[1] == 'delete':
            delete()
        elif sys.argv[1] == 'scan':
            connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
            table = connection.table(TABLE_NAME_S)
            i = 0
            for k, v in table.scan():
                print i, k, v,
                i += 1
                if i > 10: break
            
            print "\n"
            print "\n"
            print "\n"

            table = connection.table(TABLE_NAME_BB)
            i = 0
            for k, v in table.scan():
                print i, k, v, 
                i += 1
                if i > 10: break

    print 'Done...'


