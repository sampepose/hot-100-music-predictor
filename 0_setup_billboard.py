import happybase
import sys

from variables import MACHINE, VUID, BBRD_TABLE, BBRD_COLUMN_FAMILY


def setup():
    print 'Creating...'
    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    connection.create_table(BBRD_TABLE, {BBRD_COLUMN_FAMILY: dict()})


def delete():
    print 'Deleting...'
    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    connection.delete_table(BBRD_TABLE, True)


if __name__ == '__main__':
    if len(sys.argv) == 2:
        if sys.argv[1] == 'create':
            setup()
        elif sys.argv[1] == 'delete':
            delete()
        elif sys.argv[1] == 'scan':
            connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
            table = connection.table(BBRD_TABLE)
            i = 0
            for k, v in table.scan():
                print i, k, v,
                i += 1
                if i > 10: break
    else:
        print 'Not enough params'
    print 'Done...'
