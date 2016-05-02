from discogs_data.getMoreDiscogs import getMoreDiscogs
from discogs_data.preprocessMore import preprocessMore

from discogs_data.0_setup_discogs_more import setup
from discogs_data.0_setup_discogs_more import delete 


# This will grab any relevant information from the discogs database
# for new songs added to the dependent table
def addDiscogsData():
	
	try:
		delete()
	except:
		# Table already didn't exist
		pass
		
	create()
	
	print("Fetching new Discogs data")
	getMoreDiscogs()
	print("Done fetching Discogs data")
	
	print("Incorporating new Discogs data into full table")
	preprocessMore()
	print("New Discogs data successfully added")
	
if __name__ == '__main__':
	addDiscogsData()
		