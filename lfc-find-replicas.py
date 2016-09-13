#!/usr/bin/python

import subprocess
import sys
import os
import lfc
from optparse import OptionParser

DEFAULT_LFC_HOST = 'lfc1.egee.cesnet.cz'

DEFAULT_DEST = 'srm://golias100.farm.particle.cz/dpm/farm.particle.cz/home/auger/'

DEFAULT_OUTPUT_FILE = 'lfc-replication-file'

USAGE = '%prog [options] [-r end_destination] path_prefix'


def get_shell_output(cmd):
    return subprocess.Popen(cmd.split(" "), stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()

if __name__ == "__main__":

    opts = OptionParser(usage=USAGE)
    opts.add_option('-r', '--replication', dest='destination', default=DEFAULT_DEST,
                    help='Generate a file with source and destination using this destination')
    opts.add_option('-o', '--outputFile', dest='outFile', default=DEFAULT_OUTPUT_FILE)
    (options, args) = opts.parse_args()

    if len(args) < 1:
        print opts.print_usage()
        sys.exit(1)

    if 'LFC_HOST' not in os.environ:
        os.environ['LFC_HOST'] = DEFAULT_LFC_HOST

    lfc_path_prefix = args[0]
    if lfc_path_prefix[-1] == '/':
        parent_dir = lfc_path_prefix[:-1]
    full_output, full_error = get_shell_output("lfc-ls -lR %s" % parent_dir)

    if full_error:
        print 'LFC query failed: "%s"' % full_error
        sys.exit(2)
    else:
        print "LFC query succeeded, parsing output..."

    parent_dir = ""
    lfns = []
    for line in full_output.split('\n'):
        # skip empty lines
        if not line:
            continue

        # if first char is '/', set the parrent dir
        if line[0] == '/':
            parent_dir = line[:line.rfind(':')] # e.g. line: /grid/auger/prod/B2015FixedETIronEpos_gr352:
            if parent_dir[-1] == '/':
                parent_dir = parent_dir[:-1]

        # line describes a file
        if line[0] == '-':
            lfns.append(parent_dir + "/" + filter(None, line.split(" "))[-1])

    print "Obtained a list of %d lfns, now getting replicas..." % len(lfns)
    with open(options.outFile, 'w') as output :
        for lfn in lfns:
            # get surl
            result, replica_list = lfc.lfc_getreplica(lfn, "", "")

            if result == 0:
                lineArr = []
                for i in replica_list:
                    lineArr.append(i.sfn)
                dest = options.destination + lfn
                if dest in lineArr:
                    continue
                lineArr.append(dest)
                output.write(" ".join(lineArr) + '\n')

    print "File constructed, see %s" % options.outFile
