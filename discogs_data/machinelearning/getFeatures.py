import discogs_client
import happybase
import json
import unicodedata
import re
import numpy as np

from sklearn.feature_extraction.text import CountVectorizer

from variables import  DCOG_TABLE, DCOG_COLUMN_FAMILY, DCOG_COLUMN, MACHINE, VUID

def getFeatures():
    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    table = connection.table(DCOG_TABLE)
    
    genre_data = []
    style_data = []
    
    keys = []

    for key,d in table.scan():
        data = json.loads(d.itervalues().next())
        genres = data['genres']
        styles = data['styles']
        
        if (genres):
            genre_data.append(genres)
        else:
            genre_data.append(' ')
            
        if (styles):
            style_data.append(styles)
        else:
            style_data.append(' ')

        keys.append(key)


    g_vectorizer = CountVectorizer(analyzer = "word",   \
                                   tokenizer = None,    \
                                   preprocessor = None, \
                                   stop_words = None) 
    
    g_features = g_vectorizer.fit_transform(genre_data)
    g_features = g_features.toarray()

    s_vectorizer = CountVectorizer(analyzer = "word",   \
                                   tokenizer = None,    \
                                   preprocessor = None, \
                                   stop_words = None)
    
    s_features = s_vectorizer.fit_transform(style_data)
    s_features = s_features.toarray()

    k_arr = np.array(keys)
    k_arr.shape = (-1, 1)

    features = np.concatenate((k_arr, g_features, s_features), axis=1)

    return features


if __name__ == '__main__':
    print getFeatures()
