#!/usr/bin/env python2.7

"""
Watchers for task result file in jobtracker history dir, parses them and imports into DB.
"""
import os
import sys
import logging
import time

from counters.lib import parser
from counters.lib import data
from counters.lib import watcher
from counters.lib import tasks
from counters.lib import settings

opts = settings.init ()

os.chdir (opts['WorkDir'])

tasks.init (opts['TaskClassesFile'])

JT_History = opts['JobTrackerHistoryDir']
watch_interval = int (opts['WatcherInterval'])

logging.basicConfig (format="%(asctime)s: %(message)s", level=logging.INFO)

w = watcher.HadoopWatcher (JT_History, JT_History + "/done", opts['WatcherStateFile'])
importer = data.CounterDataImporter ()

iteration = 0

while True:
    logging.info ("Process watching directories")
    fresh = w.process ()
    if len (fresh) > 0:
        logging.info ("Done, got %d fresh entries, parse them" % len (fresh))
        for je in fresh:
            if not je.exists ():
                logging.info ("Gone away %s, skip it" % je.counter_file)
                continue
            logging.info ("Parse %s" % je.config_file)
            counterParser = parser.CounterFileParser (je.counter_file)
            counterParser.parse ()
            if "JOBID" in counterParser.jobInfo.vals:
                pool = parser.getPoolFromJobConfig (je.config_file)
                if pool != None:
                    counterParser.setPool (pool)
                    if importer.isValidJobInfo (counterParser.jobInfo):
                        taskInstance = importer.handleJobInfo (counterParser)
                        importer.handleCounters (taskInstance, counterParser)
                    else:
                        logging.info ("Job info is not complete yet, skip")
                else:
                    logging.warn ("Pool not found, skip")
            else:
                logging.warn ("JobID not found, skip")
            je.processed ()
        w.checkpoint ()
    else:
        logging.info ("Done, nothing changed")

    time.sleep (watch_interval)
    iteration += 1

    # periodically, perform maintanence tasks
    if iteration % 10 == 0:
        logging.info ("Maintenance: cleanup zombie tasks")
        ttid = data.getLatestTTID ()
        if ttid == None:
            continue
        zombies = w.findZombies (ttid)
        if len (zombies) > 0:
            logging.info ("Found %d zombie jobs" % len (zombies))
            data.buryZombies (zombies)
            logging.info ("Zombies destroyed!")
        else:
            logging.info ("No zombies found")

