# File Transfer scripts developed for the Auger experiment

## lfc-find-replicas

Usage: `lfc-find-replicas.py [options] [-r end_destination] path_prefix`

This script is used for listing all the files in a subdirectory tree under the specified directory (parameter `path_prefix` 
and finding their replicas. The motivation for this script was to enable users to easily transfer data from 
a particular production to one SE. 
 
The output is by default a list of replicas. When only a list of LFNs is wanted, use the `-L` switch. 

By default the output is printed on console, if the user wants to redirect it, `-o output_file` option can be used.

Because the script was designed to work closely with the other scripts in this repository, the user can define 
a destination SE by using the `-r end_destination` option. The output then would be a list of files, one line per file. On each line
the first url would be the current replica (other replicas will follow if available), the last will be the current LFN 
pasted after the destination given by the user (the destination has to be the whole url prefix e.g. 'srm://golias100.farm.particle.cz/dpm/farm.particle.cz/home/auger/'). 
This output is suitable as an input for the script `submitFTS.py`. If there is a file, which replica is already at the destination, it won't be listed in the output.

## submitFTS

Usage: `submitFTS.py [options] [-r source destination] filename [filename]*`

This script transforms an input file with urls into one or more FTS jobs. In the input file there should be at least 2 urls per 
line, the first is recognized as the source, the second as the destination of the transfer. If there are more possible
source replicas they can be also mentioned in the same line:

`source-replica-url [source-replica-url] destination-replica-url`

When the `-r source destination` switch is used, the script expects the input file to contain only one string per line.
The string then specifies the url suffix of the file. Source and destination specified as switches parameters are then
the url prefixes of the source and destination SEs (again, the whole path prefix has to be used in order for the paths to 
be found). **ATTENTION**: When used, this switch has to be the last switch before the filenames.

Description of the rest of the input parameters is printed out when running the script with the `-h` switch.
 
When the job(s) are submitted, their jobIDs are appended to the file specified by the `-j` switch (default ~/jobIDs).

When using this command to submit a job, that registers files into the LFC, the option `-O` or `--overwrite` has to be used
(reason for this is the implementation of the LFC plugin for FTS). This option is optional for regular file transfers.

## registerAndResubmit

Usage: `registerAndResubmit.py [options] jobID`

This script is used to handle the results of the transfer jobs: the files that were transferred successfully should be registered
 in the LFC, files that were not transferred for various reasons should be resubmitted in another FTS job. The user must specify
 the jobID. The script will then prompt the user whether it should submit a registration job and a follow-up job, if needed. If
 the original job is not finished, the script will inform the user and terminate.
 
Description of the rest of the input parameters is printed out when running the script with the `-h` switch.

## Example usage
```
$ ./lfc-find-replicas.py -o transfer-input-file -r 'srm://golias100.farm.particle.cz/dpm/farm.particle.cz/home/auger/' /grid/auger/prod/B2015FixedETIronEpos_gr352/en18.500/th38.000/095487

$ ./submitFTS.py -s "https://fts3-pilot.cern.ch:8446" -j exampleJobIDs -S AUGERPROD transfer-input-file

# now a replication job has been submitted (or more, one job per 100 files). Progress of replication can be seen on the endpoints web (usually endpoint address with port 8449)
$ cat exampleJobIDs 
transfer-input-file
	b5dcd6a4-96cd-11e6-b08c-02163e01841b

# let's check the job before it is done
$ ./registerAndResubmit.py -s "https://fts3-pilot.cern.ch:8446" b5dcd6a4-96cd-11e6-b08c-02163e01841b
Sorry, job b5dcd6a4-96cd-11e6-b08c-02163e01841b has not finished yet, its' status is ACTIVE

# after N minutes
$ ./registerAndResubmit.py -s "https://fts3-pilot.cern.ch:8446" b5dcd6a4-96cd-11e6-b08c-02163e01841b
The job had problems, its' status is FINISHEDDIRTY
The registration job is ready now, do you wish to submit it? [Y/n]
The registration job ID is 1eb23e70-96cf-11e6-b12f-02163e00a39b
A job(s) for a retry transfer of the untransfered files is ready, do you wish to submit it? [Y/n] N

# from the web interface I saw, that the error on failed transfers was 'Destination file exists and overwrite is not enabled' so I did not submit a retry job
```

## grepFileByError

Usage: `grepFileByError.py [options] jobID error[ error]`

The script is used to extract transfers that failed due to a particular error. The user can describe multiple error in 
form of a space separated list (each error reason should be surrounded in quotes). The script outputs the pairs of 
source destination SURLs, so it can be easily used as output of the `submitFTS` script. The output is printed 
on the console.

If the user is not sure about the correct phrasing of the error, the script shall be ran with the `-u` option, which
prints all the error reasons encountered in that job.

Description of the rest of the input parameters is printed out when running the script with the `-h` switch.