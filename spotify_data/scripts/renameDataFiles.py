import os

mydir = os.getcwd()

for i in os.listdir(mydir):
	if (i[0] != 'r'):
		# format: csv-regional-us-daily-2015-04-252000.csv
		newfile = i[22:32] + ".csv"
		os.rename(i, newfile)
		# print i + " --> " + newfile