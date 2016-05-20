#!/usr/bin/python
import sys
# import json
import fts3.rest.client.easy as fts3

# espPrefix = "srm://se-cafpegrid.ugr.es%s"
# cesnetPrefix = "srm://dpm1.egee.cesnet.cz/dpm/cesnet.cz/%s"
# cesnetPrefix = "srm://golias100.farm.particle.cz/dpm/farm.particle.cz/%s"

ENDPOINT = 'https://fts3-pilot.cern.ch:8446'
MAX_NUM_OF_TRANSFERS = 100
SLEEP_TIME = 1 * 60 * 60  # secs
USAGE = "%s source destination filename" % sys.argv[0]

transferJobs = []


def createTransferJob(transfers, lib, num):
    label = 'Library %s part %s' % (lib, num)
    newjob = fts3.new_job(transfers, metadata=label, retry=3, spacetoken="AUGERPROD", reuse=False, verify_checksum=False)
    newjob['params']["timeout"] = 7200
    return newjob


if __name__ == "__main__":
    context = fts3.Context(ENDPOINT)
    failedFiles = open('/home/adammar/failed.files', 'a')
    if len(sys.argv) < 4:
        print USAGE
    sourcePrefix = sys.argv[1]
    destinationPrefix = sys.argv[2]
    for filename in sys.argv[3:]:
        with open(filename) as inputF:
            files = inputF.readlines()

        numOfTransfers = 0
        numOfJobs = 1
        transferList = []
        for f in files:
            fs = f.split(' ')[0].split('/')
            # if 'small.tar' in fs[-1]:
            #     continue

            try:
                suffix = '/'.join(fs[fs.index('home'):])
            except ValueError:
                suffix = '/'.join(fs)
                #print (f)
                #failedFiles.write(f)
                #continue

            sourceURI = sourcePrefix + suffix
            destinationURI = destinationPrefix + suffix

            transfer = fts3.new_transfer(sourceURI, destinationURI)
            transferList.append(transfer)
            numOfTransfers += 1

            if numOfTransfers > MAX_NUM_OF_TRANSFERS:
                numOfTransfers = 0
                job = createTransferJob(transferList, filename, str(numOfJobs))
                # print json.dumps(job, indent=2)
                # jobID = 'abc'
                jobID = fts3.submit(context, job)
                transferJobs.append(jobID)
                numOfJobs += 1
                transferList = []

        # submit the last job
        job = createTransferJob(transferList, filename, str(numOfJobs) + " last")
        # print json.dumps(job, indent=2)
        # jobID = 'abc'
        jobID = fts3.submit(context, job)
        transferJobs.append(jobID)

        with open('/home/adammar/jobIDs', 'a') as f:
            f.write(filename + '\n')
            for jobID in transferJobs:
                f.write('\t' + jobID + '\n')

    failedFiles.close()

# job_status = fts3.get_job_status(context, jobID, list_files=True)
