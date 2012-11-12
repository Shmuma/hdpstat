from django.db import connection


def pools_resources_all_time ():
    """
    Returns list with pool resources used. Result is a list of dicts with pool and time entries
    """
    sql = "select p.name, sum(cv.value) as time_ms from counters_counter c, counters_countervalue cv, counters_taskinstance ti, counters_pool p  where c.tag in ('MAP_WALL_CLOCK_MS', 'REDUCE_WALL_CLOCK_MS') and cv.counter_id = c.id and ti.id = cv.taskinstance_id and p.id = ti.pool_id group by p.id order by p.name"

    cur = connection.cursor ()
    cur.execute (sql)
    return [{'pool': ent[0], 'time': long (ent[1])} for ent in cur.fetchall ()]
    
