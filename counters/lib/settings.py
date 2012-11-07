"""
Global settings reader
"""
import json
import os.path


def read (fname):
    """
    Parses settings file and return dict object with them
    """
    fname = os.path.expanduser (fname)
    if not os.path.exists (fname):
        return None

    with open (fname, "r") as fd:
        return json.load (fd)
