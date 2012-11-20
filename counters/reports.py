import sys
import datetime
import time

from django.utils import timezone
from django.db import connection

from models import TaskInstance, CounterValue, Counter


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


def jobs_history (pool=None, user=None, status=None, cgroup="Time", dt_from=None, dt_to=None):
    sql = """select ti.jobid, p.name, u.name, ti.submitted, ti.finished, ti.status,
c.tag as counter_tag, c.name as counter_name, cv.value
from counters_taskinstance ti, counters_pool p, counters_user u,
counters_countervalue cv, counters_counter c, counters_countergroup cg
where p.id = ti.pool_id and u.id = ti.user_id and cv.taskInstance_id = ti.id and
c.id = cv.counter_id and cg.id = c.counterGroup_id and cg.name = %s
"""
    # handle filters
    sqlargs = [cgroup]
    if pool:
        sql += " and p.name = %s"
        sqlargs.append (pool)
    if user:
        sql += " and u.name = %s"
        sqlargs.append (user)
    if status:
        sql += " and ti.status = %s"
        sqlargs.append (TaskInstance.statusValue (status))
    if dt_from:
        sql += " and ti.started >= %s"
        sqlargs.append (dt_from)
    if dt_to:
        sql += " and ti.started <= %s"
        sqlargs.append (dt_to)

    cur = connection.cursor ()
    cur.execute (sql, sqlargs)
    sql += " order by ti.jobid"

    result = {}
    tags = set ()

    for entry in cur.fetchall ():
        jobid, pool_name, user_name, submitted, finished, status, counter_tag, counter_name, value = entry
        if not jobid in result:
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
            result[jobid] = cur_result

        # handle value
        result[jobid][counter_tag] = value
        tags.add (counter_tag)

    # if task is missing some value, add it with zero value
    for v in result.values ():
        for missing in tags - set (v.keys ()):
            v[missing] = 0L

    return result.values ()
