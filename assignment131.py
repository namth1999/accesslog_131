#!/usr/bin/python3
import re
import sys
from datetime import datetime, timedelta
from os import walk
import os.path
from map_reduce_lib import *


def mapper_ip_hits(line):
    """ Map function for the word count job.
    Splits line into words, removes low information words (i.e. stopwords) and outputs (key, 1).
    """
    # process_print('is processing `%s`' % line)
    output = []
    data = line.split()
    if len(data) == 10:
        ip, id, user, dt, timezone, method, path, proto, status, size = data
        dt = datetime.strptime(dt.replace("[", ""), "%d/%b/%Y:%H:%M:%S")
        timezone = datetime.strptime(timezone.replace("]", ""), "-%H00").hour
        dt = dt + timedelta(hours=timezone)
        output.append((ip, '%s#%s' % (dt, 1)))
    return output


def reducer_ip_hits(key_value_item):
    """ Reduce function for the word count job.
    Converts partitioned shakespeare (key, [value]) to a summary of form (key, value).
    """
    key, values = key_value_item
    time_list = []
    total_count = 0

    for v in values:
        time, count = v.split('#')
        total_count += int(count)
        time_list.append(datetime.strptime(time, "%Y-%m-%d %H:%M:%S").month)

    time_list = sorted(time_list)
    print(time_list)
    total_time = time_list[-1] - time_list[0]
    if total_time == 0:
        hit_period = total_count
        return key, '%s/month' % hit_period
    else:
        hit_period = total_count / total_time
        return key, '%s/month' % hit_period


if __name__ == '__main__':
    file_contents = []
    filenames = []
    # Parse command line arguments
    if len(sys.argv) == 1:
        print('Please provide a text-file that you want to perform the wordcount on as a command line argument.')
        sys.exit(-1)
    elif os.path.isdir(sys.argv[1]):
        filenames = next(walk(sys.argv[1]), (None, None, []))[2]
        for fn in filenames:
            with open('%s/%s' % (sys.argv[1], fn), 'r') as input_file:
                file_contents.extend(input_file.read().splitlines())
    elif os.path.isfile(sys.argv[1]):
        with open(sys.argv[1], 'r') as input_file:
            file_contents.extend(input_file.read().splitlines())
    elif not os.path.isdir(sys.argv[1]) or not os.path.isfile(sys.argv[1]):
        print('File or dir `%s` not found.' % sys.argv[1])
        sys.exit(-1)

    # Execute MapReduce job in parallel.
    map_reduce = MapReduce(mapper_ip_hits, reducer_ip_hits, 8)
    hits = map_reduce(file_contents, debug=True)

    print('Access log:')
    for ip, period in hits:
        print('{0}\t{1}'.format(ip, period))
