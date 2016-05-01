import happybase
import sys

from variables import MACHINE, VUID, TABLE_NAME_S, COLUMN_FAMILY_S, COLUMN_S, TABLE_NAME_BB, COLUMN_FAMILY_BB, COLUMN_BB


def setup():
    print 'Creating...'
    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    connection.create_table(TABLE_NAME_S, {COLUMN_FAMILY_S: dict()})
    connection.create_table(TABLE_NAME_BB, {COLUMN_FAMILY_BB: dict()})


def delete():
    print 'Deleting...'
    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    connection.delete_table(TABLE_NAME_S, True)
    connection.delete_table(TABLE_NAME_BB, True)


if __name__ == '__main__':
    if len(sys.argv) == 2:
        if sys.argv[1] == 'create':
            setup()
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


            print "\n\n\n"
            table = connection.table(TABLE_NAME_BB)
            i = 0
            for k, v in table.scan():
                print i, v, k, 
                i += 1
                if i > 10: break
    else:
        print 'Not enough params'
    print 'Done...'
