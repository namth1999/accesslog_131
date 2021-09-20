#!/usr/bin/python3
import re
import sys
from datetime import datetime, timedelta
from os import walk
import os.path
from map_reduce_lib import *


def mapper_ip_hits(line):
    """ Map function for assignment 3.
    Sync between timezone and and outputs (key, timezone,1).
    """
    # process_print('is processing `%s`' % line)
    output = []
    data = line.split()
    if len(data) == 10:
        ip, id, user, dt, timezone, method, path, proto, status, size = data
        # sync between timezone
        dt = datetime.strptime(dt.replace("[", ""), "%d/%b/%Y:%H:%M:%S")
        timezone = datetime.strptime(timezone.replace("]", ""), "-%H00").hour
        dt = dt + timedelta(hours=timezone)
        output.append((ip, '%s#%s' % (dt, 1)))
    return output


def reducer_ip_hits(key_value_item):
    """ Reduce function for assignment 3 .
    Converts partitioned data (key, [value]) to a summary of form (key, hit period).
    """
    key, values = key_value_item
    time_list = []
    total_count = 0

    for v in values:
        time, count = v.split('#')
        total_count += int(count)
        time_list.append(datetime.strptime(time, "%Y-%m-%d %H:%M:%S"))
    #Sort the result list
    time_list = sorted(time_list)
    start_date = time_list[0]
    end_date = time_list[-1]
    # time differ of the first and the last hit
    time_delta = end_date.month - start_date.month + 1
    if start_date.month == end_date.month:
        # same year same month
        if start_date.year == end_date.year:
            hit_period = total_count
            return key, '%s/month' % hit_period
        # diff year same month
        else:
            total_periods = 12 * (end_date.year - start_date.year)
            hit_period = total_count / total_periods
            return key, '%s/month' % hit_period
    else:
        # same year diff month
        if start_date.year == end_date.year:
            hit_period = total_count / time_delta
            return key, '%s/month' % hit_period
        # diff year diff month
        else:
            total_periods = 12 * (end_date.year - start_date.year) + time_delta
            hit_period = total_count / total_periods
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
