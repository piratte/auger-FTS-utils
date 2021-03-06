#!/usr/bin/python
import json
import sys
# import json
import fts3.rest.client.easy as fts3
from os.path import expanduser
from optparse import OptionParser


DEFAULT_ENDPOINT = 'https://fts3-pilot.cern.ch:8446'
DEFAULT_SPACETOKEN = ""  # "AUGERPROD"
MAX_NUM_OF_TRANSFERS = 499
USAGE = '%prog [options] [-r source destination] filename [filename]*'

transferJobs = []


def createTransferJob(transfers, lib, num, overwrite):
    label = 'Library %s part %s' % (lib, num)
    newjob = fts3.new_job(transfers, metadata=label, retry=3, spacetoken=options.spacetoken, reuse=False,
                          verify_checksum=options.computeChecksum, overwrite=overwrite)
    newjob['params']["timeout"] = 7200
    return newjob


def getPathSuffixes(f):
    source_path_arr = f.split(' ')[0].split('/')
    dest_path_arr = source_path_arr

    try:
        source_suffix = '/'.join(source_path_arr[source_path_arr.index('home'):])
        dest_suffix = '/'.join(dest_path_arr[dest_path_arr.index('home'):])
    except ValueError:
        source_suffix = '/'.join(source_path_arr)
        dest_suffix = '/'.join(dest_path_arr)

    return source_suffix, dest_suffix


def getReplicationPaths(f, sourcePref, destinationPref):

    if sourcePref[-1] != '/': sourcePref += '/'
    if destinationPref[-1] != '/': destinationPref += '/'

    sourceSuffix, destSuffix = getPathSuffixes(f)
    resultSourceURI = sourcePref + sourceSuffix
    resultDestinationURI = destinationPref + destSuffix

    return resultSourceURI, resultDestinationURI

if __name__ == "__main__":
    # get user home directory
    home = expanduser("~")

    opts = OptionParser(usage=USAGE)
    opts.add_option('-s', '--endpoint', dest='endpoint', default=DEFAULT_ENDPOINT,
                    help='Specify FTS endpoint, default = ' + DEFAULT_ENDPOINT)
    opts.add_option('-S', '--dst-spacetoken', dest='spacetoken', default=DEFAULT_SPACETOKEN,
                    help='Specify the target spacetoken, default: ' + DEFAULT_SPACETOKEN)
    opts.add_option('-j', '--job-id-file', dest='jobIdFile', default=home + '/jobIDs',
                    help='Specify the file to which the jobIDs will be appended, default: ' + home + '/jobIDs')
    opts.add_option('--dry-run', dest='dryRun', default=False, action='store_true',
                    help='Print the json input of the FTS jobs on console')
    opts.add_option('-c', '--checksum', dest='computeChecksum', default=False, action='store_true',
                    help='Enforce checksum checks')
    opts.add_option('-O', '--overwrite', dest='overwriteFlag', default=False, action='store_true',
                    help='Enforce overwritting files (necessary for a registration job)')
    opts.add_option('-r', '--replication', dest='replication', default=False, action='store_true',
                    help='The input file contains only one filename per line. '
                         'The file should be copied to the same path on a different SE')
    (options, args) = opts.parse_args()

    context = fts3.Context(options.endpoint)
    if (options.replication and len(args) < 3) or (not options.replication and len(args) < 1):
        print USAGE
        sys.exit(1)

    if options.replication:
        sourcePrefix = args[0]
        destinationPrefix = args[1]
        fileArgInd = 2
    else:
        fileArgInd = 0

    for filename in args[fileArgInd:]:
        with open(filename) as inputF:
            currentFileLines = inputF.readlines()

        numOfTransfers = 0
        numOfJobs = 1
        transferList = []
        for curLine in currentFileLines:
            if options.replication:
                sourceURI, destinationURI = getReplicationPaths(curLine, sourcePrefix, destinationPrefix)
                transfer = fts3.new_transfer(sourceURI, destinationURI)
            else:
                lineArr = curLine.split()
                sourceURI = lineArr[0]
                destinationURI = lineArr[-1]
                transfer = fts3.new_transfer(sourceURI, destinationURI)
                if len(lineArr) > 2:
                    for lineIndex in range(1, len(lineArr)-1):
                        fts3.add_alternative_source(transfer, lineArr[lineIndex])

            transferList.append(transfer)
            numOfTransfers += 1

            if numOfTransfers > MAX_NUM_OF_TRANSFERS:
                numOfTransfers = 0
                job = createTransferJob(transferList, filename, str(numOfJobs), options.overwriteFlag)
                if options.dryRun:
                    print json.dumps(job, indent=2)
                    jobID = 'abc'
                else:
                    jobID = fts3.submit(context, job)
                transferJobs.append(jobID)
                numOfJobs += 1
                transferList = []

        # submit the last job
        if transferList:
            job = createTransferJob(transferList, filename, str(numOfJobs) + " last", options.overwriteFlag)
            if options.dryRun:
                print json.dumps(job, indent=2)
                jobID = 'abc'
            else:
                jobID = fts3.submit(context, job)
            transferJobs.append(jobID)

        # if some jobs were submitted, note their IDs
        if not options.dryRun:
            with open(options.jobIdFile, 'a') as curLine:
                curLine.write(filename + '\n')
                for jobID in transferJobs:
                    curLine.write('\t' + jobID + '\n')
