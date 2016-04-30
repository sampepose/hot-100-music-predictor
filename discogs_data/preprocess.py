import discogs_client
import happybase
import json
import unicodedata

from variables import DEPENDENT_TABLE, DEPENDENT_COLUMN_FAMILY, DCOG_TABLE, DCOG_COLUMN_FAMILY, DCOG_COLUMN, MACHINE, VUID

connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
table = connection.table(DCOG_TABLE)

count = 0

a_count = 0
g_count = 0
s_count = 0

for key,d in table.scan():
    data = json.loads(d.itervalues().next())
    alias = data['alias']
    genres = data['genres']
    styles = data['styles']
    count += 1

    final = {}

    # Clean up alias catagory
    if(alias):
        a_count += 1
        final['alias'] = alias
    else:
        final['alias'] = None

    # Create string of all genres if they exist 
    if(genres):
        g_count += 1
        f_genres = ' '.join(genres)
        final['genres'] = f_genres
    else:
        final['genres'] = None

    # Create string of all styles if they exist 
    if(styles):
        s_count += 1
        f_styles = ' '.join(styles)
        final['styles'] = f_styles
    else:
        final['styles'] = None
    
    table.put(key, {DCOG_COLUMN_FAMILY +':' + DCOG_COLUMN : json.dumps(final)})

    print key


print "PROCESSING COMPLETE"
print "{} songs processed".format(count)
print "{} artists found with aliases".format(a_count)
print "{} songs with a genre or more found".format(g_count)
print "{} songs with a style or more found".format(s_count)

