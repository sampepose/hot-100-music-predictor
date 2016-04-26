from pyspark import SparkConf, SparkContext

import happybase
import sys
import json #not positive if need, data is CSV right now

from variables import MACHINE, VUID, PAGE_TABLE, INDEX_TABLE, COLUMN_FAMILT, COLUMN

'''
Store in hbase: word -> {'column_family:page_title': {'title': title, 'word': word, 'tfidf': tfidf, 'pr': pr}}
1. Create (title, ((title, word, tf-idf), page_rank)) -- this requires a join.
2. Group by word (will need a map before being able to group)
3. Foreach() group, write to hbase using the store function. 
4. The the scan function in step 0 to check results.
Hbase put info:
http://happybase.readthedocs.org/en/latest/user.html#performing-batch-mutations
'''

def hbase(spark):
    #get files blah blah
    myfile = ''

    step1 = myfile.foreach(lambda (x,y) : store(x,y))

def store(word, info_list):
    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    table = connection.table(INDEX_TABLE)
    b = table.batch()

    for title, tfidf, pr, in info_list:
        data = {}
        data['title']=title
        data['word']=word
        data['tfidf']=tfidf
        data['pr']=pr
        # Put data into hbase in the specified format
        b.put(word, {COLUMN_FAMILY + ':' + title:
        json.dumps(data)})
    b.send()
                                                                                

if __name__ == '__main__':
    conf = SparkConf()
    if sys.argv[1] == 'local':
        conf.setMaster("local[3]")
        print 'Running locally'
    elif sys.argv[1] == 'cluster':
        conf.setMaster("spark://10.0.22.241:7077")
        print 'Running on cluster'

    conf.set("spark.executor.memory", "10g")
    conf.set("spark.driver.memory", "10g")
    spark = SparkContext(conf = conf)
    hbase(spark)


