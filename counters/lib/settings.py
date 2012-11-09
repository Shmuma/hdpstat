"""
Global settings reader
"""
import json
import os.path

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
