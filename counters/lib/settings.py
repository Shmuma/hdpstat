"""
Global settings reader
"""
import json
import os.path
import sys

from hdpstat.settings import DATABASES

def read (fname):
    """
    Parses settings file and return dict object with them
    """
    fname = os.path.expanduser (fname)
    if not os.path.exists (fname):
        return None

    with open (fname, "r") as fd:
        return json.load (fd)


def updateDjango (opts):
    """
    Change Django DB settings according to options
    """
    DATABASES['default']['NAME'] = opts['DBName']
    DATABASES['default']['USER'] = opts['DBUser']
    DATABASES['default']['PASSWORD'] = opts['DBPass']


def init ():
    settings_file = "~/.hdpstat"

    opts = read (settings_file)
    if opts == None:
        print "Error: config file '%s' not found! Please, create it" % settings_file
        sys.exit (1)

    updateDjango (opts)
    return opts
