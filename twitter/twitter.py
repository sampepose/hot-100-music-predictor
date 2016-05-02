from pyspark import SparkConf, SparkContext
from operator import add
import os
import json
import happybase
from dateutil.parser import parse
from variables import MACHINE, VUID, DEPENDENT_TABLE, TWITTER_TABLE, TWITTER_COLUMN_FAMILY, TWITTER_COLUMN

twitter_files = 'hdfs:///tmp/tweets/*'

def parseTweet(tweet):
    try:
        return json.loads(tweet)
    except ValueError:
        return None

def main(spark):
    # Load tweets from HDFS
    #tweets = spark.textFile(twitter_files).map(parse).filter(lambda row: row is not None) \
    #    .map(lambda tweet: tweet['text'].lower().strip())

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
    data = spark.parallelize(rows)
    
    # <title, (alias1,alias,...)> and <artist, (alias1,alias2,...)>
    titles = data.map(lambda row: row['title'].lower().strip()).distinct() \
        .map(lambda title: (title, [title, ''.join(title.split())]))
    artists = data.map(lambda row: row['artist'].lower().strip()).distinct() \
        .map(lambda artist: (artist, [artist, ''.join(artist.split())]))
    terms = titles.union(artists)

    def unpack(tweet):
        text = ''
        created_at = ''
        if 'text' in tweet:
            text = tweet['text'].lower().strip()
        if 'created_at' in tweet:
            created_at = parse(tweet['created_at']).date().isoformat()
        else:
            print "No created at...", tweet
        return (text, created_at)

    tweets = spark.textFile(twitter_files).map(parseTweet).filter(lambda row: row is not None) \
        .map(unpack)#(lambda tweet: (tweet['text'].lower().strip(), parse(tweet['created_at']).date().isoformat()))

    def processCross(params):
        tweet = params[0][0]
        date = params[0][1]
        origtitle = params[1][0]
        titles = params[1][1]
        hashtags = [word[1:] for word in tweet.split() if word.startswith("#")]
        for title in titles:
            if title in tweet or title in hashtags:
                yield (origtitle, (date, 1))
        yield (origtitle, (date, 0))
   
    def r((v1, c1), (v2, c2)):
        return c1 + c2

    term_date_count = tweets.cartesian(terms) \
        .flatMap(processCross)
        
    date_totalcounts = term_date_count.map(lambda (origtitle, (date, cnt)): (date, cnt)) \
        .reduceByKey(add) \
        .collectAsMap()

    print date_totalcounts

    term_counts = term_date_count.map(lambda (origtitle, (date, cnt)): (origtitle, cnt)) \
        .reduceByKey(add)

    term_weighted_counts = term_date_count.map(lambda (origtitle, (date, cnt)): (origtitle, float(cnt) / (1.0 + date_totalcounts[date]))) \
        .reduceByKey(add)

    result = term_counts.join(term_weighted_counts).collect()

    twitter_table = connection.table(TWITTER_TABLE)
    b = twitter_table.batch()
    for row in result:
        key = TWITTER_COLUMN_FAMILY + ":" + TWITTER_COLUMN
        b.put(row[0], {key: json.dumps({'item': row[0], 'count': row[1][0], 'weightedcount': row[1][1]})})
    b.send()                 

if __name__ == '__main__':
    conf = SparkConf()
    #conf.setMaster("local[3]")
    conf.setMaster("spark://10.0.22.241:7077")
    conf.set("spark.executor.memory", "10g")
    conf.set("spark.driver.memory", "10g")
    spark = SparkContext(conf=conf)
    spark.addPyFile(os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'variables.py')))
    main(spark)
