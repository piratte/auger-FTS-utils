#!/usr/bin/python
import json
import sys
# import json
import fts3.rest.client.easy as fts3
from os.path import expanduser
from optparse import OptionParser


ENDPOINT = 'https://fts3-pilot.cern.ch:8446'
DEFAULT_SPACETOKEN = "AUGERPROD"
MAX_NUM_OF_TRANSFERS = 100
USAGE = "%s source destination filename" % sys.argv[0]

transferJobs = []


def createTransferJob(transfers, lib, num):
    label = 'Library %s part %s' % (lib, num)
    newjob = fts3.new_job(transfers, metadata=label, retry=3, spacetoken=options.spacetoken, reuse=False,
                          verify_checksum=options.computeChecksum)
    newjob['params']["timeout"] = 7200
    return newjob


def getPathSuffixes(f, isReplic):

    source_path_arr = f.split(' ')[0].split('/')
    if isReplic:
        dest_path_arr = source_path_arr
    else:
        dest_path_arr = f.split(' ')[1].split('/')

    # if 'small.tar' in fs[-1]:
    #     continue

    try:
        source_suffix = '/'.join(source_path_arr[source_path_arr.index('home'):])
        dest_suffix = '/'.join(dest_path_arr[dest_path_arr.index('home'):])
    except ValueError:
        source_suffix = '/'.join(source_path_arr)
        dest_suffix = '/'.join(dest_path_arr)

    return source_suffix, dest_suffix


if __name__ == "__main__":
    # get user home directory
    home = expanduser("~")

    opts = OptionParser()
    opts.add_option('-s', '--endpoint', dest='endpoint', default=ENDPOINT)
    opts.add_option('-S', '--dst-spacetoken', dest='spacetoken', default=DEFAULT_SPACETOKEN)
    opts.add_option('--dry-run', dest='dry_run', default=False, action='store_true')
    opts.add_option('-c', '--checksum', dest='computeChecksum', default=False, action='store_true')
    opts.add_option('-r', '--replication', dest='replication', default=False, action='store_true',
                    help='The input file contains only one filename per line. '
                         'The file should be copied to the same path on a different SE')
    (options, args) = opts.parse_args()

    context = fts3.Context(options.endpoint)
    if len(sys.argv) < 4:
        print USAGE
        sys.exit(1)

    # failedFiles = open(home + '/failed.files', 'a')

    sourcePrefix = sys.argv[1]
    destinationPrefix = sys.argv[2]
    for filename in sys.argv[3:]:
        with open(filename) as inputF:
            files = inputF.readlines()

        numOfTransfers = 0
        numOfJobs = 1
        transferList = []
        for f in files:
            sourceSuffix, destSuffix = getPathSuffixes(f, options.replication)

            sourceURI = sourcePrefix + sourceSuffix
            destinationURI = destinationPrefix + destSuffix

            transfer = fts3.new_transfer(sourceURI, destinationURI)
            transferList.append(transfer)
            numOfTransfers += 1

            if numOfTransfers > MAX_NUM_OF_TRANSFERS:
                numOfTransfers = 0
                job = createTransferJob(transferList, filename, str(numOfJobs))
                if options.dry_run:
                    print json.dumps(job, indent=2)
                    jobID = 'abc'
                else:
                    jobID = fts3.submit(context, job)
                transferJobs.append(jobID)
                numOfJobs += 1
                transferList = []

        # submit the last job
        job = createTransferJob(transferList, filename, str(numOfJobs) + " last")
        if options.dry_run:
            print json.dumps(job, indent=2)
            jobID = 'abc'
        else:
            jobID = fts3.submit(context, job)
        transferJobs.append(jobID)

        # if some jobs were submitted, note their IDs
        if not options.dry_run:
            with open(home + '/jobIDs', 'a') as f:
                f.write(filename + '\n')
                for jobID in transferJobs:
                    f.write('\t' + jobID + '\n')

    # failedFiles.close()

# job_status = fts3.get_job_status(context, jobID, list_files=True)