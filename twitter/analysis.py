import json
import happybase
import numpy as np
from variables import MACHINE, VUID, TWITTER_TABLE

import matplotlib as mpl
mpl.use('agg')
import matplotlib.pyplot as plt

def main():
    connection = happybase.Connection(MACHINE + '.vampire', table_prefix=VUID)
    table = connection.table(TWITTER_TABLE)
    
    rows = []
    counts = []
    for key, data in table.scan():
        data = json.loads(data.itervalues().next())
        rows.append(data)
        counts.append(data['count'])
    rows = sorted(rows, key=lambda k: k['count'], reverse=True)
    counts = np.array(sorted(counts, reverse=True))
    counts_no_zero = filter(lambda a: a != 0, counts)
    print "Max:", rows[0:10]
    print "Middle:",rows[1000:1010]
    print "Min:", rows[-10:]
    print "Zeros:", len(counts) - len(counts_no_zero)
    print "Count:",len(counts)
    print "Avg (no 0s):",np.mean(counts_no_zero)
    print "Median (no 0s):",np.median(counts_no_zero)
    print np.histogram(counts_no_zero, np.arange(0, 500, 10))

    f = plt.figure()
    h = plt.hist(counts_no_zero, np.arange(0,500,10))
    print np.sum(h[0])
    plt.title('Histogram of item mentions')
    plt.xlabel('Item mentions')
    plt.ylabel('Occurence')
    f.savefig('plot.png')

if __name__ == '__main__':
    main()
