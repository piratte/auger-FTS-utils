# File Transfer scripts developed for the Auger experiment

## lfc-find-replicas

Usage: `lfc-find-replicas [options] [-r end_destination] path_prefix`

This script is used for listing all the files in a subdirectory tree under a specified directory (parameter `path_prefix` 
and finding their replicas. The motivation for this script was to enable users to easily transfer data from 
a particular production to one SE. 
 
The output is by default a list of replicas. When only a list of LFNs is wanted, use the `-L` switch. 

By default the output is printed on console, if the user wants to redirect it, `-o output_file` option can be used.

Because the script was designed to work closely with the other scripts in this repository, the user can define 
a "destination" by using the `-r destination` option. The output then would be a list of files, one LFN per line. On each line
the first url would be the current replica (or more if there is more than one available), the last will be the LFN 
pasted after the destination given by the user. This output is suitable as an input for the script `submitFTS.py`.