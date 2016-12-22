#!/usr/bin/python

import sys
import fts3.rest.client.easy as fts3
import os
from optparse import OptionParser

LFCHOST = "lfc://lfc1.egee.cesnet.cz/"
DEAFUT_ENDPOINT = 'https://fts3-pilot.cern.ch:8446'

MAX_NUM_OF_TRANSFERS = 500

USAGE = '%prog [options] jobID'



def query_yes_no(question, default="yes"):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = raw_input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")


if __name__ == "__main__":
    home = os.path.expanduser('~')

    opts = OptionParser(usage=USAGE)
    opts.add_option('-s', '--endpoint', dest='endpoint', default=DEAFUT_ENDPOINT)
    opts.add_option('-j', '--job-id-file', dest='jobIdFile', default=home + '/jobIDs',
                    help='Specify the file to which the jobIDs will be appended, default: ' + home + '/jobIDs')
    opts.add_option('--reg-job-id-file', dest='regJobIdFile', default=home + '/regJobIDs',
                    help='Specify the file to which the jobIDs will be appended, default: ' +home + '/regJobIDs')
    opts.add_option('--lfc-host', dest='lfcHost', default=LFCHOST,
                    help='Specify the LFC host, default: ' + LFCHOST)
    (options, args) = opts.parse_args()

    if len(args) < 1:
        opts.print_usage()
        sys.exit(1)
    job_id = args[0]

    context = fts3.Context(options.endpoint)

    job_status = fts3.get_job_status(context, job_id, list_files=True)
    if job_status['job_state'] not in ['FINISHED', 'FINISHEDDIRTY', 'CANCELED', 'FAILED']:
        print "Sorry, job %s has not finished yet, its' status is %s" % (job_id, job_status['job_state'])
        sys.exit(0)

    if job_status['job_state'] != 'FINISHED':
        print "The job had problems, its' status is %s" % job_status['job_state']

    transferedFiles = [f['dest_surl'] for f in job_status['files']
                       if f['file_state'] in ['FINISHED']
                       or f['reason'] == 'DESTINATION file already exists and overwrite is not enabled']

    transferList = []
    for filename in transferedFiles:
        fs = filename.split('/')
        lfcURI = '/'.join(fs[fs.index('grid'):])
        transfer = fts3.new_transfer(filename, options.lfcHost + lfcURI)
        transferList.append(transfer)

    if len(transferList) > 0:
        job = fts3.new_job(transferList, metadata="Registration of the files transfered by job " + job_id,
                           overwrite=True)
        if query_yes_no("The registration job is ready now, do you wish to submit it?"):
            jobID = fts3.submit(context, job)
            print "The registration job ID is " + jobID
            with open(options.regJobIdFile, 'a') as f:
                f.write('\t' + jobID + '\n')
    notFinishedTransfers = [fts3.new_transfer(f['source_surl'], f['dest_surl']) for f in job_status['files']
                            if f['file_state'] in ['FAILED', 'CANCELED']
                            and f['reason'] != 'DESTINATION file already exists and overwrite is not enabled']

    if len(notFinishedTransfers) < 1:
        sys.exit(0)

    jobChunks = [notFinishedTransfers[i:i + MAX_NUM_OF_TRANSFERS] for i in
                 xrange(0, len(notFinishedTransfers), MAX_NUM_OF_TRANSFERS)]

    jobs = []
    for chunkOfTransfers in jobChunks:
        newjob = fts3.new_job(chunkOfTransfers, spacetoken=job_status['space_token'],
                              overwrite=job_status['overwrite_flag'], retry=job_status['retry'],
                              reuse=job_status['reuse_job'], metadata=job_status['job_metadata'], verify_checksum=False)
        # double the internal transfer timeout
        newjob['params']["timeout"] = 7200
        jobs.append(newjob)
    if query_yes_no("A job(s) for a retry transfer of the untransfered files is ready, do you wish to submit it?"):
        jobIDs = []
        for job in jobs:
            jobIDs.append(fts3.submit(context, job))

        print "The retry job IDs are " + str(jobIDs)
        with open(options.jobIdFile, 'a') as output:
            output.write('Retry for ' + job_id + '\n')
            for jobID in jobIDs:
                output.write('\t' + jobID + '\n')
