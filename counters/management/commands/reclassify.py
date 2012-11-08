"""
Tasks classifier - reclassify all tasks from DB
"""
import os
import sys

from lib import db
from lib import tasks
from lib import settings

settings_file = "~/.hdpstat"

opts = settings.read (settings_file)
if opts == None:
    print "Error: config file '%s' not found! Please, create it" % settings_file
    sys.exit (1)

os.chdir (opts['WorkDir'])
tasks.init (opts['TaskClassesFile'])

dbc = db.DBConnection (opts['DBHost'], opts['DBUser'], opts['DBPass'], opts['DBName'])
total = 0
updated = 0

for task in dbc.allTasks ():
    group = tasks.classify (task['taskName'])
    total += 1
    if group != task['groupName']:
        groupID = dbc.lookupTaskGroup (group)
        if groupID != task['groupID']:
            dbc.updateTaskGroup (task['id'], groupID)
            print "Task '%s' assigned to group '%s'" % (task['taskName'], group)
            updated += 1

print "Processed %d tasks, updated %d" % (total, updated)


