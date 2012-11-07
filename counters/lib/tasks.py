"""
By task name determine task group.
"""
import json
import os.path
import re

# rules database
db = {}


def init (rulesFilename):
    global db

    if os.path.exists (rulesFilename):
        with open (rulesFilename, 'r') as fd:
            db = json.load (fd)


def classify (jobName):
    """
    Given job name, return task group name.
    """
    for group, rules in db.iteritems ():       
        if type (rules) == list:
            for rule in rules:
                if re.match (rule, jobName):
                    return group
        elif type (rules) == unicode:
            if re.match (rules, jobName):
                return group
    return "OtherTaskGroup"
