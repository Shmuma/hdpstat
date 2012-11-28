import sys
import datetime
import time

from django.utils import timezone
from django.db import connection

from models import TaskInstance, CounterValue, Counter, CounterGroup


def get_resource_counter_ids ():
    """
    Lookup IDs of MAP_WALL_CLOCK_MS and REDUCE_WALL_CLOCK_MS counters. Return them in a tuple
    """
    return (Counter.objects.get (tag="MAP_WALL_CLOCK_MS").id, Counter.objects.get (tag="REDUCE_WALL_CLOCK_MS").id)


def get_jobs_resource_usages (map_counter_id, red_counter_id, where_part="1=1", query_args=None):
    """
    Generates tuples (jobid, pool_name, mappers_time, reducers_time, map_start, map_finish, red_start, red_finish)
    """
    sql = """select ti.jobid, pool.name as pool_name, coalesce(cv1.value, 0) as map_time,
	     coalesce(cv2.value, 0) as red_time,
             ti.started_maps, ti.finished_maps, ti.started_reducers, ti.finished_reducers
	     from counters_pool pool, counters_taskinstance ti
	     left outer join counters_countervalue cv1
	     on cv1.taskinstance_id = ti.id and cv1.counter_id=%(map_counter_id)d
	     left outer join counters_countervalue cv2
	     on cv2.taskinstance_id = ti.id and cv2.counter_id=%(red_counter_id)d
	     where pool.id = ti.pool_id and %(where_part)s""" % {'map_counter_id': map_counter_id, 'red_counter_id': red_counter_id,
                                                                 'where_part': where_part}
    cur = connection.cursor ()
    cur.execute (sql, query_args)
    for entry in cur.fetchall ():
        yield entry


def pools_resources_all_time ():
    """
    Returns list with pool resources used. Result is a list of dicts with pool and time entries
    """
    map_counter_id, reduce_counter_id = get_resource_counter_ids ()

    # pool name -> result dict
    resources = {}

    for jobid, pool_name, map_time, red_time, map_start, map_finish, red_start, red_finish in get_jobs_resource_usages (map_counter_id, reduce_counter_id):
        pool_data = resources.get (pool_name, {'pool': pool_name, 'time': 0L, 'map_time': 0L, 'reduce_time': 0L})
        pool_data['time'] += map_time + red_time
        pool_data['map_time'] += map_time
        pool_data['reduce_time'] += red_time
        resources[pool_name] = pool_data
    return resources.values ()


def get_overlap_fraction (int_a, int_b):
    """
    Returns fraction of b shared with a
    """
    if int_b[0] == None or int_b[1] == None:
        return 0.0

    def inside (val, interval):
        return val >= interval[0] and val <= interval[1]

    b_len = (int_b[1] - int_b[0]).total_seconds ()

    # inside
    if inside (int_b[0], int_a) and inside (int_b[1], int_a):
        return 1.0
    # left
    if inside (int_b[1], int_a) and int_b[0] < int_a[0]:
        return (int_b[1] - int_a[0]).total_seconds () / b_len
    # right
    if inside (int_b[0], int_a) and int_b[1] > int_a[1]:
        return (int_a[1] - int_b[0]).total_seconds () / b_len
    # outside
    if inside (int_a[0], int_b) and inside (int_a[1], int_b):
        return (int_a[1] - int_a[0]).total_seconds () / b_len

    # don't overlap
    return 0.0


def pools_resources_interval (dt_from, dt_to):
    """
    Return the some as all_time report, but limited by time interval
    """
    map_counter_id, reduce_counter_id = get_resource_counter_ids ()

    # pool name -> result dict
    resources = {}

    queries = {
        # 1. falls in interval entirely
        'inside':
            {'where_part': "ti.started >= %s and ti.finished <= %s",
             'query_args': [dt_from, dt_to]},
        # 2. intersect left bound
        'left':
            {'where_part': "ti.started < %s and ti.finished <= %s and ti.finished > %s",
             'query_args': [dt_from, dt_to, dt_from]},
        # 3. intersect right bound
        'right':
            {'where_part': "ti.started >= %s and ti.started <= %s and ti.finished > %s",
             'query_args': [dt_from, dt_to, dt_to]},
        # 4. intersect both bounds
        'both':
            {'where_part': "ti.started < %s and ti.finished > %s",
             'query_args': [dt_from, dt_to]},
        }

    for qargs in queries.values ():
        for jobid, pool_name, map_time, red_time, map_start, map_finish, red_start, red_finish in get_jobs_resource_usages (map_counter_id, reduce_counter_id, **qargs):
            map_fraction = get_overlap_fraction ((dt_from, dt_to), (map_start, map_finish))
            red_fraction = get_overlap_fraction ((dt_from, dt_to), (red_start, red_finish))

            pool_data = resources.get (pool_name, {'pool': pool_name, 'time': 0L, 'map_time': 0L, 'reduce_time': 0L})
            pool_data['time'] += map_time * map_fraction + red_time * red_fraction
            pool_data['map_time'] += map_time * map_fraction
            pool_data['reduce_time'] += red_time * red_fraction
            resources[pool_name] = pool_data

    return resources.values ()


def test_interval ():
    dt_to = datetime.datetime (year=2012, month=11, day=1, tzinfo=timezone.get_current_timezone ())
    dt_from = dt_to - datetime.timedelta (days=3)
    report = pools_resources_interval (dt_from=dt_from, dt_to=dt_to)
    return report


def jobs_history (pools=None, users=None, statuses=None, cgroup="Time",
                  dt_from=None, dt_to=None, job_name=""):
    # build list with counter group tags
    default_tags = {}
    for counter in CounterGroup.objects.get (name=cgroup).counter_set.all ():
        default_tags[counter.tag] = 0L

    # here we get list of task instances
    ti_sql = """select ti.id, ti.jobid, p.name, u.name, ti.submitted, ti.finished, ti.status
from counters_taskinstance ti, counters_pool p, counters_user u, counters_task t
where p.id = ti.pool_id and u.id = ti.user_id and t.id = ti.task_id
"""
    # handle filters
    ti_sqlargs = []
    if pools:
        conds = ["p.name = %s"] * len (pools)
        ti_sql += "and (" + " or ".join (conds) + ")"
        ti_sqlargs += pools
    if users:
        conds = ["u.name = %s"] * len (users)
        ti_sql += "and (" + " or ".join (conds) + ")"
        ti_sqlargs += users
    if statuses:
        conds = ["ti.status = %s"] * len (statuses)
        ti_sql += "and (" + " or ".join (conds) + ")"
        ti_sqlargs += map (TaskInstance.statusValue, statuses)
    if dt_from:
        ti_sql += " and ti.started >= %s"
        ti_sqlargs.append (dt_from)
    if dt_to:
        ti_sql += " and ti.started <= %s"
        ti_sqlargs.append (dt_to)
    if len (job_name) > 0:
        ti_sql += " and t.name like %s"
        ti_sqlargs.append ("%"+job_name+"%")
    ti_sql += " order by ti.jobid"
    cur = connection.cursor ()
    cur.execute (ti_sql, ti_sqlargs)

    # maps id in jobinstances to result dict
    result = {}
    ids = []

    for entry in cur.fetchall ():
        recid, jobid, pool_name, user_name, submitted, finished, status = entry
        ids.append (recid)
        if not recid in result:
            if finished == None:
                duration = datetime.datetime.now (timezone.get_current_timezone ()) - submitted
            else:
                duration = finished - submitted

            cur_result = dict ({
                'jobid': jobid,
                'pool': pool_name,
                'user': user_name,
                'submitted': submitted,
                'duration': duration,
                'status': dict (TaskInstance.STATUSES).get (status),
                })
            cur_result.update (default_tags)
            result[recid] = cur_result

    # select values of this counter
    if len (ids) > 0:
        c_sql = """select cv.taskInstance_id, c.tag, cv.value
    from counters_countervalue cv, counters_counter c, counters_countergroup cg
    where cv.taskInstance_id in (%s) and c.id = cv.counter_id and cg.id = c.counterGroup_id
    """ % ",".join (map (str, ids))

        c_sql += " and cg.name = %s"

        cur = connection.cursor ()
        cur.execute (c_sql, [cgroup])

        for recid, tag, value in cur.fetchall ():
            if recid in result:
                result[recid][tag] = value
    return result.values ()


def job_detail_data (jobid):
    res = []
    ti = TaskInstance.objects.get (jobid=jobid)
    res.append (('JobID', ti.jobid))
    res.append (('Name', ti.task.name))
    res.append (('Group', ti.task.taskGroup.name))
    res.append (('Pool', ti.pool.name))
    res.append (('User', ti.user.name))
    res.append (('Submitted', ti.submitted))
    res.append (('Started', ti.started))
    res.append (('Finished', ti.finished))
    now = datetime.datetime.now (timezone.get_current_timezone ())

    if ti.finished != None:
        res.append (('Duration', ti.finished-ti.started))
    else:
        res.append (('Duration', now-ti.started))
    res.append (('Status', ti.get_status_display ()))

    res.append (('Mappers', ti.mappers))
    res.append (('Reducers', ti.reducers))

    res.append (('Mappers started', ti.started_maps))
    res.append (('Mappers finished', ti.finished_maps))
    if ti.started_maps == None:
        res.append (('Mappers duration', None))
    else:
        if ti.finished_maps == None:
            res.append (('Mappers duration', now-ti.started_maps))
        else:
            res.append (('Mappers duration', ti.finished_maps-ti.started_maps))

    res.append (('Reducers started', ti.started_reducers))
    res.append (('Reducers finished', ti.finished_reducers))
    if ti.started_reducers == None:
        res.append (('Reducers duration', None))
    else:
        if ti.finished_reducers == None:
            res.append (('Reducers duration', now-ti.started_reducers))
        else:
            res.append (('Reducers duration', ti.finished_reducers-ti.started_reducers))

    return [{'name': v[0], 'value': v[1]} for v in res]


def job_counters_data (jobid):
    """
    Return data with job's counters for detail page rendering
    """
    ti = TaskInstance.objects.get (jobid=jobid)

    res = []
    seen_groups = set ()

    for cv in ti.countervalue_set.order_by ("counter__counterGroup", "counter__tag"):
        group_name = cv.counter.counterGroup.name

        if group_name in seen_groups:
            group_name = unicode (" ")
        else:
            seen_groups.add (group_name)

        res.append ({'group': group_name, 'name': cv.counter.name, 'value': long (cv.value),
                     'origin_value': long (cv.value)})

    return res
