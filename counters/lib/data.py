from ..models import Pool, User, TaskGroup, Task, TaskInstance, CounterGroup, Counter, CounterValue

from django.utils import timezone
import logging
import datetime

import tasks
import counters
import parser


def hadoopTimestampToDT (ts):
    """
    Convert Hadoop timestamp to datetime. If TS is zero, return None
    """
    if ts == 0:
        return None
    else:
        return datetime.datetime.fromtimestamp (ts/1000.0, timezone.get_current_timezone ())


def getLatestTTID ():
    qs = TaskInstance.objects.order_by ('-jobid')
    if qs.count () == 0:
        return None
    jid = qs[0].jobid
    return "_".join (jid.split ("_")[0:2])


def buryZombies (jobIDs):
    """
    Mark all running jobs from given list as KILLED
    """
    status = TaskInstance.statusValue ('KILLED')
    TaskInstance.objects.filter (jobid__in=jobIDs).update (status=status)


class CounterDataImporter (object):
    def isValidJobInfo (self, jobInfo):
        vals = ["JOBID", 'POOL', 'USER']

        for v in vals:
            if not v in jobInfo.vals:
                return False

        return True


    def handleJobInfo (self, counterParser):
        jobInfo, tasksDict = counterParser.jobInfo, counterParser.tasks

        jobid = jobInfo.vals["JOBID"]
        pool = Pool.objects.get_or_create (name=jobInfo.vals["POOL"])[0]
        user = User.objects.get_or_create (name=jobInfo.vals["USER"])[0]

        # deal with timestamps
        submitted = hadoopTimestampToDT (long (jobInfo.vals.get ("SUBMIT_TIME", 0)))
        started = hadoopTimestampToDT (long (jobInfo.vals.get ("LAUNCH_TIME", 0)))
        finished = hadoopTimestampToDT (long (jobInfo.vals.get ("FINISH_TIME", 0)))

        map_tasks = [t for t in tasksDict.values () if t.taskType == "MAP"]
        red_tasks = [t for t in tasksDict.values () if t.taskType == "REDUCE"]
        started_maps, finished_maps = map (hadoopTimestampToDT, parser.getTasksTimeInterval (map_tasks))
        started_reducers, finished_reducers = map (hadoopTimestampToDT, parser.getTasksTimeInterval (red_tasks))

        mappers = long (jobInfo.vals.get ("TOTAL_MAPS", 0))
        reducers = long (jobInfo.vals.get ("TOTAL_REDUCES", 0))
        status = jobInfo.vals.get ("JOB_STATUS", "UNKNOWN")
        taskName = jobInfo.vals.get ('JOBNAME', "")

        taskGroup = TaskGroup.objects.get_or_create (name=tasks.classify (taskName))[0]
        task = Task.objects.get_or_create (name=taskName[:250], taskGroup=taskGroup)[0]

        taskData = {
            'task': task,
            'pool': pool,
            'user': user,
            'submitted': submitted,
            'started': started,
            'finished': finished,
            'started_maps': started_maps,
            'finished_maps': finished_maps,
            'started_reducers': started_reducers,
            'finished_reducers': finished_reducers,
            'mappers': mappers,
            'reducers': reducers,
            'status': TaskInstance.statusValue (status),
            }
        taskInstance, created = TaskInstance.objects.get_or_create (jobid=jobid, defaults=taskData)
        if not created:
            # check for task changed
            changed = False
            for k in taskData.keys ():
                if taskData[k] != getattr (taskInstance, k):
                    setattr (taskInstance, k, taskData[k])
                    changed = True
            if changed:
                taskInstance.save ()

        return taskInstance


    def handleCounters (self, taskInstance, counterParser):
        """
        Updates intermediate counter values for a job, then merge final jobInfo counters
        """
        jobInfo, tasks = counterParser.jobInfo, counterParser.tasks

        # delete existing counters
        CounterValue.objects.filter (taskInstance=taskInstance).delete ()

        # aggregate counters
        result = {}		# tag -> value mapping
        blacklist = set ()
        whitelist = set ()

        def processCounter (result, blacklist, whitelist, counter):
            """
            Return True if counter triplet (group, tag, value) must be
            included in result list
            """
            gname, tag, val = counter
            if tag in blacklist:
                return

            white = tag in whitelist
            if white:
                result[tag] = result.get (tag, 0) + long (val)
            else:
                group = counters.classify (tag)
                if group != None:
                    result[tag] = result.get (tag, 0) + long (val)
                    whitelist.add (tag)
                else:
                    blacklist.add (tag)

        # all counter values are summed up
        for t in tasks.values ():
            t.processCounters (lambda c: processCounter (result, blacklist, whitelist, c))

        # then, we process final counters form jobInfo, but only not present in a list
        if jobInfo.counters != None:
            for gname, tag, val in jobInfo.counters:
                if not tag in result:
                    group = counters.classify (tag)
                    if group != None:
                        result[tag] = long (val)

        counterValueList = []
        for tag, val in result.iteritems ():
            if val == 0:
                continue
            counterGroup = CounterGroup.objects.get_or_create (name=counters.classify (tag))[0]
            counter = Counter.objects.get_or_create (tag=tag, name=counters.name (tag),
                                                     counterGroup=counterGroup)[0]
            counterValueList.append (CounterValue (taskInstance=taskInstance, counter=counter, value=val))
        if len (counterValueList) > 0:
            CounterValue.objects.bulk_create (counterValueList)
