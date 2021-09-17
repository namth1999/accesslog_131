#!/usr/bin/python3

import sys
import os.path
import multiprocessing
import string
import operator
from map_reduce_lib import *
from datetime import datetime


def mapper(line):
    # process_print('is processing `%s`' % line)
    output = []

    data = line.strip().split(" ")
    if len(data) == 10:
        ip, identity, username, time, zone, method, path, protocol, status, size = data
        time = time.strip("[")
        year_month = datetime.strptime(time, '%d/%b/%Y:%H:%M:%S').strftime('%Y-%m')
        output.append((ip, year_month))

    return output


def reducer(key_value_item):
    """ Reduce function for the word count job.
    Converts partitioned shakespear (key, [value]) to a summary of form (key, value).
    """

    key, values = key_value_item

    totalHits = 0
    oldKey = None
    for val in values:
        curKey = val
        if oldKey and oldKey != curKey:
            totalHits = 0
        oldKey = curKey
        totalHits += 1


    return key, oldKey, totalHits


if __name__ == '__main__':
    # Parse command line arguments
    if len(sys.argv) == 1:
        print('Please provide a text-file that you want to perform the wordcount on as a command line argument.')
        sys.exit(-1)
    elif not os.path.isfile(sys.argv[1]):
        print('File `%s` not found.' % sys.argv[1])
        sys.exit(-1)

    # Read data into separate lines.
    with open(sys.argv[1], 'r') as input_file:
        file_contents = input_file.read().splitlines()

    # Execute MapReduce job in parallel.
    map_reduce = MapReduce(mapper, reducer, 8)
    results = map_reduce(file_contents, debug=True)

    # Sort the result by IP
    results.sort(key=operator.itemgetter(0))

    print('Ip hits per month/year:')
    for ip, year_month, hits in results:
        print('{0}\t{1}\t{2}'.format(ip, year_month, hits))
