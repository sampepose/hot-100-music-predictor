import discogs_client
import happybase
import json

from variables import DEPENDENT_TABLE, DEPENDENT_COLUMN_FAMILY, DCOG_TABLE, DCOG_COLUMN_FAMILY, DCOG_COLUMN, MACHINE, VUID

d = discogs_client.Client('ExampleApplication/0.1', user_token="RAzmbwmoidsSvCfkfKKnWoRpYnLKGsYMVwsXvhrZ")

connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
dcog_table = connection.table(DCOG_TABLE)
dpd_table = connection.table(DEPENDENT_TABLE)

count = 0
artist_fail = 0
style_fail = 0

for key,data in dpd_table.scan():
    dpd_data = json.loads(data.itervalues().next())
    title = dpd_data['title']
    artist = dpd_data['artist']

    # Try to search for artist
    try:
        artist_search = d.search(artist, type = 'artist')
        alias = [art.name for art in  artist_search[0].aliases]
    except:
        artist_fail += 1
        alias = None
        
    # Try to search for release that contained the song title
    try:
        style_search = d.search(title + ' ' + artist, type = 'release')
        styles = style_search[0].styles
        genres = style_search[0].genres
    except:
        style_fail += 1
        styles = None
        genres = None

    # If all succeeded
    if(alias and styles and genres):
        count +=1

    dcog_data = {'alias':alias, 'styles':styles,'genres':genres}
    dcog_table.put(key, {DCOG_COLUMN_FAMILY +':' + DCOG_COLUMN : json.dumps(dcog_data)})



print("Success Count:  " + count)
print("Artist Failure Count:  " + artist_fail)
print("Sytle Failure Count:  " + style_fail)
