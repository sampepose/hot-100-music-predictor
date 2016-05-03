import discogs_client
import happybase
import json
import unicodedata
import re
import numpy as np

from sklearn.feature_extraction.text import CountVectorizer

from variables import  DCOG_TABLE, DCOG_COLUMN_FAMILY, DCOG_COLUMN, DCOG_F2_TABLE, DCOG_F2_COLUMN_FAMILY, DCOG_F2_COLUMN, MACHINE, VUID

def makeFeatures_2():

    def averageRow(arr):
        total = 0
        count = 0
        for item in arr:
            if item != 0:
                total += item
                count += 1
        if count == 0:
            return 0
        else:
            return total/float(count)

    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    table = connection.table(DCOG_TABLE)
    f_table = connection.table(DCOG_F2_TABLE)
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

    # Vectorize genre word counts
    g_vectorizer = CountVectorizer(analyzer = "word",   \
                                   tokenizer = None,    \
                                   preprocessor = None, \
                                   stop_words = None) 
    
    g_features = g_vectorizer.fit_transform(genre_data)
    g_features = g_features.toarray()

    # Try weighting the word counts in each song by the occurance
    # of that genre across the data set, then averaging for one value per song 
    g_counts = np.sum(g_features, axis=0)
    g_scaled = g_counts * g_features
    g_means = np.apply_along_axis(averageRow, 1, g_scaled) 
    g_means = np.array(g_means)
    g_means.shape = (-1,1)

    # Vectorize style word counts
    s_vectorizer = CountVectorizer(analyzer = "word",   \
                                   tokenizer = None,    \
                                   preprocessor = None, \
                                   stop_words = None)
    
    s_features = s_vectorizer.fit_transform(style_data)
    s_features = s_features.toarray()

    # Try weighting the word counts in each song by the occurance                                                                                                                                                 
    # of that genre across the data set, then averageing for one value per song  
    s_counts = np.sum(s_features, axis=0)
    s_scaled = s_counts * s_features
    s_means = np.apply_along_axis(averageRow, 1, s_scaled)
    s_means = np.array(s_means)
    s_means.shape = (-1,1)

    # Create Key Vector 
    k_arr = np.array(keys)
    k_arr.shape = (-1, 1)

    features_2 = np.concatenate((k_arr, g_means, s_means), axis=1)

    b = f_table.batch()
    for row in features_2:
        data = row[1:]
        data = list(data.astype(int))
        b.put(row[0], {DCOG_F2_COLUMN_FAMILY + ':' + DCOG_F2_COLUMN : json.dumps(data)})
   
    b.send()


if __name__ == '__main__':
    makeFeatures()
