import datetime

from django.utils import timezone
from django.db import connection

from models import TaskInstance, CounterValue


def pools_resources_all_time ():
    """
    Returns list with pool resources used. Result is a list of dicts with pool and time entries
    """
    sql = "select p.name, sum(cv.value) as time_ms from counters_counter c, counters_countervalue cv, counters_taskinstance ti, counters_pool p  where c.tag in ('MAP_WALL_CLOCK_MS', 'REDUCE_WALL_CLOCK_MS') and cv.counter_id = c.id and ti.id = cv.taskinstance_id and p.id = ti.pool_id group by p.id order by p.name"

    cur = connection.cursor ()
    cur.execute (sql)
    return [{'pool': ent[0], 'time': long (ent[1])} for ent in cur.fetchall ()]


def pools_resources_interval (dt_from, dt_to):
    """
    Return the some as all_time report, but limited by time interval
    """
    counter_tags = ['MAP_WALL_CLOCK_MS', 'REDUCE_WALL_CLOCK_MS']

    # 1. falls in interval entirely
    ti_inside = TaskInstance.objects.filter (started__gte=dt_from, finished__lte=dt_to)
    # 2. intersect left bound
    ti_left = TaskInstance.objects.filter (started__lt=dt_from, finished__lte=dt_to, finished__gte=dt_from)
    # 3. intersect right bound
    ti_right = TaskInstance.objects.filter (started__gte=dt_from, started__lte=dt_to, finished__gt=dt_to)
    # 4. intersect both bounds
    ti_both = TaskInstance.objects.filter (started__lt=dt_from, finished__gt=dt_to)

    pools_usage = {}

    # inside tasks are simple - just count their pools resource usage
    for ti in ti_inside:
        counterValues = CounterValue.objects.filter(taskInstance=ti, counter__tag__in=counter_tags)
        value = sum (map (lambda cv: cv.value, counterValues))
        pools_usage[ti.pool.name] = pools_usage.get (ti.pool.name, 0) + value

    # left-bound tasks

    return pools_usage


def test_interval ():
    dt_to = datetime.datetime (year=2012, month=11, day=1, tzinfo=timezone.get_current_timezone ())
    dt_from = dt_to - datetime.timedelta (days=3)
    report = pools_resources_interval (dt_from=dt_from, dt_to=dt_to)
    return report
