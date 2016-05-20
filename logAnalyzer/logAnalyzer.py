#!/usr/bin/python2

import sys
from datetime import datetime, timedelta
from os import system, remove

BIN_NUM = 20


def frequency_count(itt, nr_bins, minn=None, maxx=None):
    ret = []
    if minn is None:
        minn = min(itt)
    if maxx is None:
        maxx = max(itt)
    binsize = (maxx - minn) / float(nr_bins)

    # construct bins
    ret.append([minn - binsize, minn, 0])  # -inf -> min
    for x in range(0, nr_bins):
        start = minn + x * binsize
        ret.append([start, start+binsize, 0])
    ret.append([maxx, maxx + binsize, 0])  # maxx -> inf

    # assign items to bin
    for item in itt:
        for binn in ret:
            if binn[0] <= item < binn[1]:
                binn[2] += 1
    return ret

GNUPLOT_SCRIPT = """
set terminal png size 800,600
set tics font ", 18"

set output "~/log_analy_chart.png"
set ylabel 'time(s)'
set xlabel 'transfer number'
set key off
plot '/tmp/fts-log-analyze.dat' using 1:2 with lines lw 2

set output "~/log_analy_bars.png"
set style fill solid
set ylabel 'count'
set xlabel 'avg time in bin(s)'
set key off
plot '/tmp/fts-log-analyze-bars.dat' using 1:2 with boxes
"""

# read data from file
if len(sys.argv) < 2:
    print "Usage: %s LOG_FILE_NAME" % sys.argv[0]
    sys.exit(1)
logFileName = sys.argv[1]
with open(logFileName) as f:
    data = f.read().replace('\n', ' ')

# split data into records and filter out empty lines
records = data.split("Copying")
records = filter(None, records)


times = []
for record in records:
    recAsList = record.split(" ")

    # match all the time data
    time = [field for field in recAsList if "elapsed" in field]

    # if none matched, skip this record, it is invalid
    if len(time) < 1:
        continue

    # get seconds from time string
    t = datetime.strptime(time[0], "%M:%S.%felapsed")
    millis = t.microsecond / 1000 + t.second * 1000 + t.minute * 60 * 1000
    times.append(millis/float(1000))

# create .dat file
with open('/tmp/fts-log-analyze.dat', 'w') as tmpFile:
    transferId = 0
    for t in times:
        tmpFile.write("%d %d\n" % (transferId, t))
        transferId += 1

bins = frequency_count(times, BIN_NUM)
with open('/tmp/fts-log-analyze-bars.dat', 'w') as tmpFile:
    for oneBin in bins:
        tmpFile.write("%d %d\n" % (int(sum(oneBin[0:2])/2), oneBin[2]))

with open('/tmp/fts-log-analyze.gnuplot', 'w') as GPScript:
    GPScript.write(GNUPLOT_SCRIPT)

system('gnuplot /tmp/fts-log-analyze.gnuplot')
remove('/tmp/fts-log-analyze.dat')
remove('/tmp/fts-log-analyze-bars.dat')
remove('/tmp/fts-log-analyze.gnuplot')
