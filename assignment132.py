#!/usr/bin/python3

import sys
import os.path
import multiprocessing
import string
import operator
from map_reduce_lib import *
from datetime import datetime
from os import walk
import os.path


def mapper(line):
    # process_print('is processing `%s`' % line)
    output = []

    data = line.strip().split(" ")
    if len(data) == 10:
        ip, identity, username, time, zone, method, path, protocol, status, size = data
        #Remove the [ of time column to use strptime()
        time = time.strip("[")
        #Retrieve year and month from data and return as Y-M
        year_month = datetime.strptime(time, '%d/%b/%Y:%H:%M:%S').strftime('%Y-%m')
        output.append((ip, year_month))

    return output


def reducer(key_value_item):
    """ Reduce function for the third assignment.
    Converts partitioned data (key, [value]) to a summary of form (key, year-month and totalHits).
    """

    key, values = key_value_item

    y_m =[]
    hits =[]
    totalHits = 0
    #temp key for year_month
    oldKey = None
    for val in values:
        #Current year_month value
        curKey = val
        if oldKey and oldKey != curKey:
            y_m.append(oldKey)
            hits.append(totalHits)
            totalHits = 0
        oldKey = curKey
        totalHits += 1
    if oldKey != None:
        y_m.append(oldKey)
        hits.append(totalHits)

    return key,y_m,hits


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
        # loop through the array of year_month if the website was hit by the IP in mulitple months
        for i in range(0, len(year_month)):
         print('{0}\t{1}\t{2}'.format(ip, year_month[i], hits[i]))
