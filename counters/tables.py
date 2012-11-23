import django_tables2 as tables
from django_tables2.utils import A

from counters.models import Pool

import datetime


class MillisecondColumn (tables.Column):
    """
    A column that renders milliseconds timedelta value into human-readable form
    """
    def render (self, value):
        return str (datetime.timedelta (seconds=long (value/1000)))



class TimedeltaColumn (tables.Column):
    """
    A column that renders timedeltas
    """
    def render (self, value):
        # get rid of microseconds
        td = datetime.timedelta (seconds=long (value.total_seconds ()))
        return str (td)


class LargeNumberColumn (tables.Column):
    """
    A column that renders large integer value into human-readable form (M, G, T, P, etc)
    """
    # maps power of 10 to apropriate suffix
    power_to_suffix = {
        3: 'K',
        6: 'M',
        9: 'G',
        12: 'T',
        15: 'P',
        18: 'E',
        21: 'Z',
        24: 'Y',
        1000: 'inf'}


    def __init__ (self, divider = 1.0, **extra):
        super (LargeNumberColumn, self).__init__ (**extra)
        self.divider = divider


    def render (self, value):
        if self.divider != 0:
            value /= float (self.divider)

        prev_power = 0
        prev_suffix = ''
        for power in sorted (self.power_to_suffix.keys ()):
            if value < pow (10, power):
                return "%.2f%s" % (float (value) / pow (10, prev_power), prev_suffix)
            prev_power = power
            prev_suffix = self.power_to_suffix[power]


class PoolsResourcesTable (tables.Table):
    pool = tables.Column ()
    time = LargeNumberColumn (1000)
    map_time = LargeNumberColumn (1000)
    reduce_time = LargeNumberColumn (1000)

    class Meta:
        attrs = {"class": "paleblue"}



class JobsTableBase (tables.Table):
    class Meta:
        attrs = {"class": "paleblue"}

    jobid = tables.LinkColumn ('job_detail', kwargs={'jobid': A('jobid')})
#    jobid = tables.Column ()
    status = tables.Column ()
    pool = tables.Column ()
    user = tables.Column ()

    submitted = tables.DateTimeColumn ()
    duration = TimedeltaColumn ()


def make_jobs_table (cgroup, data):
    if cgroup == "Time":
        return JobsTable_Time (data)
    elif cgroup == "TaskIO":
        return JobsTable_TaskIO (data)
    elif cgroup == "MapRed":
        return JobsTable_MapRed (data)
    elif cgroup == "HBase":
        return JobsTable_HBase (data)
    return JobsTableBase (data)


class JobsTable_Time (JobsTableBase, tables.Table):
    class Meta:
        attrs = {"class": "paleblue"}

    MAP_WALL_CLOCK_MS = LargeNumberColumn (verbose_name="Mapper seconds", divider=1000)
    REDUCE_WALL_CLOCK_MS = LargeNumberColumn (verbose_name="Reducer seconds", divider=1000) 


class JobsTable_TaskIO (JobsTableBase, tables.Table):
    class Meta:
        attrs = {"class": "paleblue"}

    FILE_BYTES_READ = LargeNumberColumn (verbose_name="Locally read")
    FILE_BYTES_WRITTEN = LargeNumberColumn (verbose_name="Locally written")
    HDFS_BYTES_READ = LargeNumberColumn (verbose_name="HDFS read")
    HDFS_BYTES_WRITTEN = LargeNumberColumn (verbose_name="HDFS written")


class JobsTable_MapRed (JobsTableBase, tables.Table):
    class Meta:
        attrs = {"class": "paleblue"}

    MAP_INPUT_RECORDS = LargeNumberColumn (verbose_name="Map input records")
    MAP_OUTPUT_RECORDS = LargeNumberColumn (verbose_name="Map output records")
    SPILLED_RECORDS = LargeNumberColumn (verbose_name="Spilled records")
    REDUCE_INPUT_RECORDS = LargeNumberColumn (verbose_name="Reduce input records")
    REDUCE_OUTPUT_RECORDS = LargeNumberColumn (verbose_name="Reduce output records")
    MAP_OUTPUT_BYTES = LargeNumberColumn (verbose_name="Map output records")
    REDUCE_SHUFFLE_BYTES = LargeNumberColumn (verbose_name="Reduce shuffle bytes")


class JobsTable_HBase (JobsTableBase, tables.Table):
    class Meta:
        attrs = {"class": "paleblue"}

    PUT_CALLS = LargeNumberColumn (verbose_name="Put calls")
    PUT_KVS = LargeNumberColumn (verbose_name="Put KVs")
    PUT_BYTES = LargeNumberColumn (verbose_name="Put bytes")
    PUT_MS = LargeNumberColumn (verbose_name="Put seconds", divider=1000)
    DELETE_CALLS = LargeNumberColumn (verbose_name="Delete calls")
    DELETE_MS = LargeNumberColumn (verbose_name="Delete sec", divider=1000)
    GET_CALLS = LargeNumberColumn (verbose_name="Get calls")
    GET_KVS = LargeNumberColumn (verbose_name="Get KVs")
    GET_BYTES = LargeNumberColumn (verbose_name="Get bytes")
    GET_MS = LargeNumberColumn (verbose_name="Get sec", divider=1000)
    SCAN_NEXT_MS = LargeNumberColumn (verbose_name="Scan next sec", divider=1000)
    SCAN_NEXT_COUNT = LargeNumberColumn (verbose_name="Scan next count")
    SCAN_ROWS = LargeNumberColumn (verbose_name="Scan rows")
    SCAN_KVS = LargeNumberColumn (verbose_name="Scan KVs")
    SCAN_BYTES = LargeNumberColumn (verbose_name="Scan bytes")
    SCAN_DISK_BYTES = LargeNumberColumn (verbose_name="Scan HFile bytes")
    SCAN_READ_MS = LargeNumberColumn (verbose_name="Scan HFile sec", divider=1000)


class JobDetailTable (tables.Table):
    class Meta:
        attrs = {"class": "paleblue"}
        orderable = False
    name = tables.Column ()
    value = tables.Column ()
