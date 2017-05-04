wgot
====

Peformant parallel GET extracted from aws-cli.

Usage: 
======

wgot [-h] [-d] [-i INPUT_FILE] [--max-redirect MAX_REDIRECT] [-O file]
            [-q] [-U agent-string] [--user USER] [--password PASSWORD]
            [--version]
            [URL [URL ...]]

Parallel HTTP 

positional arguments:
  URL                   URLs to download

optional arguments:
  -h, --help            show this help message and exit
  -d, --debug           Turn on debug output
  -i INPUT_FILE, --input-file INPUT_FILE
                        Read URLs from a local or external file. If '-' is
                        specified as file, URLs are read from the standard
                        input.
  --max-redirect MAX_REDIRECT
                        Specifies the maximum number of redirections to follow
                        for a resource. The default is 20, which is usually
                        far more than necessary.
  -O file, --output-document file
                        The documents will not be written to the appropriate
                        files, but all will be concatenated together and
                        written to file. If '-'' is used as file, documents
                        will be printed to standard output.
  -q, --quiet           Turn off output
  -U agent-string, --user-agent agent-string
                        Identify as agent-string to the HTTP server.
  --user USER
  --password PASSWORD
  --version             Print version and exit

Example of ENCODE URL = https://www.encodeproject.org/files/ENCFF335WPX/@@download/ENCFF335WPX.fastq.gz

Note: --user and --password are ACCESS_KEYS and only available for ENCODE consortium members for unreleased files.

Installation
============
1. cd into the folder "wgot"

2. run the following commands:

- python setup.py build

- sudo python setup.py install


