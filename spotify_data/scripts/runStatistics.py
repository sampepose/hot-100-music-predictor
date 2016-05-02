import unicodedata
import happybase
import json
import sys
import csv
import re
import os
import operator

from variables import MACHINE, VUID, SPOTIFY_COUNTS, SPOTIFY_COLF, SPOTIFY_COL

def getTopX(l,x,y):
    indx = 0
    d = dict() #return this
    for i in l:
        if indx >= x:
            d[i[0]] = i[1]
        if indx == y:
            return sorted(d.items(), key=lambda x: x[1], reverse=True)
        indx += 1
    return sorted(d.items(), key=lambda x: x[1], reverse=True)

connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
table = connection.table(SPOTIFY_COUNTS)

plays = dict()
days = dict()

for key, val in table.scan():
    
    data = json.loads(val.itervalues().next())
    plays[key] = float(data['plays'])
    days[key] = data['days']



plays = sorted(plays.items(), key=lambda x: x[1], reverse=True)
days = sorted(days.items(), key=lambda x: x[1], reverse=True)

top_days = getTopX(days,0,5)
top_plays = getTopX(plays,0,5)

alldays = getTopX(days,0,100000)
cnt = 0
for i in alldays:
    cnt +=1
print "Total songs:",cnt

print "Most days:"
for i in top_days:
    print i

print "Most plays:"
for i in top_plays:
    print i


