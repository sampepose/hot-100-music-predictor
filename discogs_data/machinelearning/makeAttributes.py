import discogs_client
import happybase
import json
import unicodedata
import re
import numpy as np

from sklearn.feature_extraction.text import CountVectorizer

from variables import  DCOG_TABLE, DCOG_COLUMN_FAMILY, DCOG_COLUMN, MACHINE, VUID

connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
table = connection.table(DCOG_TABLE)

genre_training = []
style_training = []

for key,d in table.scan():
    data = json.loads(d.itervalues().next())
    genres = data['genres']
    styles = data['styles']
    
    if (genres):
        genre_training.append(genres)
    else:
        genre_training.append(' ')

    if (styles):
        style_training.append(styles)
    else:
        style_training.append(' ')


print("completed read, now training")

g_vectorizer = CountVectorizer(analyzer = "word",   \
                             tokenizer = None,    \
                             preprocessor = None, \
                             stop_words = None) 
    
g_features = g_vectorizer.fit_transform(genre_training)

print(g_vectorizer.get_feature_names())
print (g_features.toarray()[0:10])

s_vectorizer = CountVectorizer(analyzer = "word",   \
                             tokenizer = None,    \
                             preprocessor = None, \
                             stop_words = None)

s_features = s_vectorizer.fit_transform(style_training)

print(s_vectorizer.get_feature_names())
print (s_features.toarray()[0:10])    

print len(g_vectorizer.get_feature_names())
print len(s_vectorizer.get_feature_names())

print sum(np.sum(g_features.toarray(),axis=0))
print sum(np.sum(s_features.toarray(),axis=0))

print "PROCESSING COMPLETE"


