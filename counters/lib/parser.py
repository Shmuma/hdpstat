import re
import os.path
import glob
import logging

class EInvalidFormat (Exception):
    pass


def fileLineParse (line, kvfun):
    """
    Parses line of key="val" and calls fun for each pair
    """
    try:
        v = line.split ('="')
        key = v[0].split (" ")[1]
        for s in v[1:]:
            val, key2 = s.split ('" ')
            kvfun (key, val)
            key = key2
    except ValueError:
        pass


def parseCounters (line):
    """
    Return list of (group, name, value) tuples
    """
    res = []

    for g in re.finditer ("\{([^{}]*)}", line):
        s = g.group (1)
        mo = re.match ("(\(([^()]*)\))+", s)
        if mo == None:
            continue
        gname = mo.group (2)
        for c in re.finditer ("\[([^\[\]]*)\]", s):
            counter = c.group (1).replace ('\(', '').replace ('\)', '')
            r = re.findall ("\(([^\(\)]*)\)", counter)
            if len (r) > 2:
                c_name, c_val = r[0], r[2]
                res.append ((gname, c_name, c_val))
    return res
    


class JobInfo (object):
    """
    Information about job
    """
    vals = {}
    keys = ["JOBID", "JOBNAME", "USER", "SUBMIT_TIME", "LAUNCH_TIME", "FINISH_TIME", "TOTAL_MAPS", "TOTAL_REDUCES", "JOB_STATUS"]
    counters = None


    def __init__ (self):
        self.vals = {}
        self.counters = None


    def parse (self, l):
        def kvfun (key, val):
            if key in self.keys:
#                if key == "FINISH_TIME":
#                    logging.info ("Parsed finish time %s for job %s, line '%s'" % (val, self.vals["JOBID"], l))
#                elif key == "JOB_STATUS":
#                    logging.info ("Parsed job status %s for job %s, line '%s'" % (val, self.vals["JOBID"], l))
                self.vals[key] = val
            elif key == "COUNTERS":
                self.counters = parseCounters (val)

            
        fileLineParse (l, kvfun)


class TaskAttemptInfo (object):
    """
    Information about task's attempts
    """
    attID = None
    status = None
    startTime = None
    finishTime = None
    counters = None


    def __init__ (self, aid):
        self.attID = aid


    def parse (self, line):
        def kvfun (key, val):
            if key == "START_TIME":
                self.startTime = val
            elif key == "FINISH_TIME":
                self.finishTime = val
            elif key == "TASK_STATUS":
                self.status = val
            elif key == "COUNTERS":
                self.counters = parseCounters (val)

        fileLineParse (line, kvfun)


    def processCounters (self, fun):
        if self.counters != None:
            map (fun, self.counters)


class TaskInfo (object):
    """
    Information about job's task
    """
    taskID = None
    status = None
    taskType = None
    startTime = None
    finishTime = None
    attempts = {}


    def __init__ (self, taskID):
        self.taskID = taskID
        self.attempts = {}


    def attempt (self, aid):
        if not aid in self.attempts:
            self.attempts[aid] = TaskAttemptInfo (aid)
        return self.attempts[aid]


    def parse (self, line):
        def kvfun (key, val):
            if key == "TASK_TYPE":
                self.taskType = val
            elif key == "START_TIME":
                self.startTime = val
            elif key == "TASK_STATUS":
                self.status = val
            elif key == "FINISH_TIME":
                self.finishTime = val

        fileLineParse (line, kvfun)


    def processCounters (self, fun):
        map (lambda att: att.processCounters (fun), self.attempts.values ())

    
    @staticmethod
    def parseID (line):
        """
        Extracts only taskid
        """
        def kvfun (res, key, val):
            global tid, aid
            if key == "TASKID":
                res["tid"] = val
            elif key == "TASK_ATTEMPT_ID":
                res["aid"] = val
        res = { "tid": None, "aid": None }
        fileLineParse (line, lambda k, v: kvfun (res, k, v))
        return res["tid"], res["aid"]


class CounterFileParser (object):
    """
    Parser of task report files. Can iteratively parse file, extracting fresh
    data since last invocation.

    We have two kinds of data:
    1. job information, such as count of mappers/reduces, start time, etc
    2. list of tasks instances with attempts and counters
    """
   
    fname = None
    jobInfo = None
    tasks = {}


    def __init__ (self, fname):
        self.fname = fname
        self.jobInfo = JobInfo ()
        self.tasks = {}


    def __str__ (self):
        return "Parser of %s, info: %s, attempts parsed: %d" % (self.fname, self.jobInfo, len (self.tasks))


    def parse (self):
        """
        Try to parse file. If new attempts parsed, return their ids in a list
        """
        fresh = set ()

        if not os.path.exists (self.fname):
            return []

        with open (self.fname) as fd:
            l = fd.readline ().strip ()
            if not l.startswith ("Meta "):
                return []
            for l in fd:
                l = l.strip ()
                if l.startswith ("Job "):
                    self.jobInfo.parse (l)
                elif l.startswith ("Task "):
                    tid, aid = TaskInfo.parseID (l)
                    if not tid in self.tasks:
                        self.tasks[tid] = TaskInfo (tid)
                        fresh.add (tid)
                    self.tasks[tid].parse (l)
                elif l.startswith ("ReduceAttempt ") or l.startswith ("MapAttempt "):
                    tid, aid = TaskInfo.parseID (l)
                    if not tid in self.tasks:
                        self.tasks[tid] = TaskInfo (tid)
                        fresh.add (tid)                       
                    att = self.tasks[tid].attempt (aid)
                    att.parse (l)   
                   
        return list (fresh)

    def setPool (self, pool):
        """
        Sets job's pool value
        """
        self.jobInfo.vals["POOL"] = pool


def lookupJobConfig (dirs, jobid):
    """
    Routine looks up for specified places and tries to find job's config
    file. Return it's name on success, None otherwise.
    """
    for d in dirs:
        l = glob.glob ("%s/*%s*.xml" % (d, jobid))
        if len (l) > 0:
            return l[0]
    return None


def getPoolFromJobConfig (confFileName):
    """
    Parses config XML and extracts pool name from it
    """
    if not os.path.exists (confFileName):
        return None

    with open (confFileName) as fd:
        for l in fd:
            pos = l.find ("<name>mapred.fairscheduler.pool</name>")
            if pos > 0:
                v = "<value>"
                pos = l.find (v)
                res = l[pos+len (v):l.find ("</value>")]
                return res
    return "default"
