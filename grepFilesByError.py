#!/usr/bin/python

import sys
import fts3.rest.client.easy as fts3
import os
from optparse import OptionParser
from pprint import pprint
import re

USAGE = '%prog [options] jobID error [error] (surround the errors in quotes)'
DEAFUT_ENDPOINT = 'https://fts3-pilot.cern.ch:8446'


def convert_reason_to_regexp(error):
    """
    For an input argument, convert it into a regexp
    :param error: String user input
    :return: regexp
    """

    error = " ".join(error.split())
    if error[0] not in ["*", "^"]:
        error = ".*" + error
    if error[-1] not in ["*", "$"]:
        error += ".*"
    return error


def handle_uniq():
    global reasons, f, r
    reasons = []
    for f in job_status['files']:
        if f['file_state'] in ['FAILED', 'CANCELED']:
            reasons.append(f['reason'])
    for r in set(reasons):
        print r


def matches(reason, wanted_reasons, verbose=False):
    for wanted in wanted_reasons:
        if re.match(wanted, reason, flags=re.IGNORECASE):
            if verbose: print reason
            return True
    return False
    # return "".join(str(reason).split()).lower() not in wanted_reasons


if __name__ == "__main__":
    opts = OptionParser(usage=USAGE)
    opts.add_option('-s', '--endpoint', dest='endpoint', default=DEAFUT_ENDPOINT)
    opts.add_option('-v', '--invert-match', dest='invert', default=False, action='store_true',
                    help='Invert the sense of matching, to select files with non-matching errors')
    opts.add_option('-u', '--unique', dest='uniq', default=False, action='store_true',
                    help='Print all the errors, that occured in the job')
    opts.add_option('--verbose', dest='verbose', default=False, action='store_true',
                    help='Print all the errors, that were matched to the input')

    (options, args) = opts.parse_args()

    # get the jobID as the last parameter
    if len(args) < 2 and not options.uniq:
        opts.print_usage()
        sys.exit(1)
    job_id = args[0]

    reasons = []
    if not options.uniq:
        for r in args[1:]:
            reasons.append(convert_reason_to_regexp(r))
    # pprint(reasons)
    context = fts3.Context(options.endpoint)

    job_status = fts3.get_job_status(context, job_id, list_files=True)
    if job_status['job_state'] not in ['FINISHED', 'FINISHEDDIRTY', 'CANCELED', 'FAILED']:
        print "Sorry, job %s has not finished yet, its' status is %s" % (job_id, job_status['job_state'])
        sys.exit(0)

    if options.uniq:
        handle_uniq()
        sys.exit(0)

    if options.invert:
        notTransferedFiles = [(f['source_surl'], f['dest_surl']) for f in job_status['files']
                              if f['file_state'] in ['FAILED', 'CANCELED']
                              and not matches(f['reason'], reasons, options.verbose)]

                              #and sanitize_error(f['reason']) not in reasons]
    else:
        notTransferedFiles = [(f['source_surl'], f['dest_surl']) for f in job_status['files']
                              if f['file_state'] in ['FAILED', 'CANCELED']
                              and matches(f['reason'], reasons, options.verbose)]

    for fileTuple in notTransferedFiles:
        print "%s %s" % fileTuple
