from pyspark import SparkConf, SparkContext
from operator import add
import os
import json
import happybase
from variables import MACHINE, VUID, DEPENDENT_TABLE, TWITTER_TABLE, TWITTER_COLUMN_FAMILY, TWITTER_ARTIST_COLUMN, TWITTER_TITLE_COLUMN

twitter_files = 'hdfs:///tmp/tweets/*'

def parse(tweet):
    try:
        return json.loads(tweet)
    except ValueError:
        return None

def main(spark):
    # Load tweets from HDFS
    tweets = spark.textFile(twitter_files).map(parse).filter(lambda row: row is not None) \
        .map(lambda tweet: tweet['text'].lower().strip())

    # http://stackoverflow.com/questions/9590382/forcing-python-json-module-to-work-with-ascii
    def ascii_encode_dict(data):
        ascii_encode = lambda x: x.encode('ascii', 'ignore') if isinstance(x, unicode) else x 
        return dict(map(ascii_encode, pair) for pair in data.items())

    # Load dependent data into RDD
    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    dep_table = connection.table(DEPENDENT_TABLE)
    rows = []
    for key, data in dep_table.scan():
        key = key.lower()
        data = json.loads(data.itervalues().next(), object_hook=ascii_encode_dict)
        rows.append(data)
    print type(rows[0]['title'])
    data = spark.parallelize(rows)
    
    # <title, (alias1,alias,...)> and <artist, (alias1,alias2,...)>
    titles = data.map(lambda row: row['title'].lower().strip()).distinct() \
        .map(lambda title: (title, [title, ''.join(title.split())]))
    artists = data.map(lambda row: row['artist'].lower().strip()).distinct() \
        .map(lambda artist: (artist, [artist, ''.join(artist.split())]))

    def processCross(params):
        tweet = params[0]
        titles = params[1][1]
        hashtags = [word[1:] for word in tweet.split() if word.startswith("#")]
        for title in titles:
            if title in tweet or title in hashtags:
                yield (params[1][0], 1)
   
    artist_tweet_counts = tweets.cartesian(artists) \
        .flatMap(processCross) \
        .reduceByKey(add) \
        .collect()
    
    title_tweet_counts = tweets.cartesian(titles) \
        .flatMap(processCross) \
        .reduceByKey(add) \
        .collect()
    
    def store(rows, c, b):
        for row in rows:
            key = TWITTER_COLUMN_FAMILY + ":" + c
            print type(row[0])
            b.put(row[0], {key: json.dumps({'item': row[0], 'count': row[1]})})

    twitter_table = connection.table(TWITTER_TABLE)
    b = twitter_table.batch()
    store(artist_tweet_counts, TWITTER_ARTIST_COLUMN, b)
    store(title_tweet_counts, TWITTER_TITLE_COLUMN, b)
    b.send()

    #print("Total tweets: ", tweets.count())

if __name__ == '__main__':
    conf = SparkConf()
    #conf.setMaster("local[3]")
    conf.setMaster("spark://10.0.22.241:7077")
    conf.set("spark.executor.memory", "10g")
    conf.set("spark.driver.memory", "10g")
    spark = SparkContext(conf=conf)
    spark.addPyFile(os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'variables.py')))
    main(spark)
