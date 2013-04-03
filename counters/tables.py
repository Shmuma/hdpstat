import django_tables2 as tables
from django_tables2.utils import A

from counters.models import Pool

import datetime

from hdpstat import table_utils


class PoolsResourcesTable (tables.Table):
    pool = tables.Column ()
    time = table_utils.LargeNumberColumn (1000)
    map_time = table_utils.LargeNumberColumn (1000)
    reduce_time = table_utils.LargeNumberColumn (1000)

    class Meta:
        attrs = {"class": "paleblue"}



class JobsTableBase (tables.Table):
    class Meta:
        attrs = {"class": "paleblue"}

    jobid = tables.LinkColumn ('job_detail', kwargs={'jobid': A('jobid')})
    group = tables.TemplateColumn ('<a title="{{ record.task_name }}">{{ value }}</a>')
    status = tables.Column ()
    pool = tables.Column ()
    user = tables.Column ()

    submitted = tables.DateTimeColumn ()
    duration = table_utils.TimedeltaColumn ()


def make_jobs_table (cgroup, data):
    if cgroup == "Time":
        return JobsTable_Time (data)
    elif cgroup == "TaskIO":
        return JobsTable_TaskIO (data)
    elif cgroup == "Mappers":
        return JobsTable_Mappers (data)
    elif cgroup == "Reducers":
        return JobsTable_Reducers (data)
    elif cgroup == "HBase:Put":
        return JobsTable_HBasePut (data)
    elif cgroup == "HBase:Del":
        return JobsTable_HBaseDel (data)
    elif cgroup == "HBase:Get":
        return JobsTable_HBaseGet (data)
    elif cgroup == "HBase:Scan":
        return JobsTable_HBaseScan (data)
    elif cgroup == "HBase:ScanRS":
        return JobsTable_HBaseScanRS (data)
    return JobsTableBase (data)


class JobsTable_Time (JobsTableBase, tables.Table):
    class Meta:
        attrs = {"class": "paleblue"}

    MAP_WALL_CLOCK_MS = table_utils.LargeNumberColumn (verbose_name="Mapper seconds", divider=1000)
    REDUCE_WALL_CLOCK_MS = table_utils.LargeNumberColumn (verbose_name="Reducer seconds", divider=1000) 


class JobsTable_TaskIO (JobsTableBase, tables.Table):
    class Meta:
        attrs = {"class": "paleblue"}

    FILE_BYTES_READ = table_utils.LargeNumberColumn (verbose_name="Locally read")
    FILE_BYTES_WRITTEN = table_utils.LargeNumberColumn (verbose_name="Locally written")
    HDFS_BYTES_READ = table_utils.LargeNumberColumn (verbose_name="HDFS read")
    HDFS_BYTES_WRITTEN = table_utils.LargeNumberColumn (verbose_name="HDFS written")


class JobsTable_Mappers (JobsTableBase, tables.Table):
    class Meta:
        attrs = {"class": "paleblue"}

    MAP_INPUT_RECORDS = table_utils.LargeNumberColumn (verbose_name="Map in records")
    MAP_OUTPUT_RECORDS = table_utils.LargeNumberColumn (verbose_name="Map out records")
    SPILLED_RECORDS = table_utils.LargeNumberColumn (verbose_name="Spilled records")
    MAP_OUTPUT_BYTES = table_utils.LargeNumberColumn (verbose_name="Map out records")


class JobsTable_Reducers (JobsTableBase, tables.Table):
    class Meta:
        attrs = {"class": "paleblue"}

    REDUCE_INPUT_RECORDS = table_utils.LargeNumberColumn (verbose_name="Reduce in records")
    REDUCE_OUTPUT_RECORDS = table_utils.LargeNumberColumn (verbose_name="Reduce out records")
    REDUCE_SHUFFLE_BYTES = table_utils.LargeNumberColumn (verbose_name="Shuffle bytes")


class JobsTable_HBasePut (JobsTableBase, tables.Table):
    class Meta:
        attrs = {"class": "paleblue"}

    PUT_CALLS = table_utils.LargeNumberColumn (verbose_name="Put calls")
    PUT_KVS = table_utils.LargeNumberColumn (verbose_name="Put KVs")
    PUT_BYTES = table_utils.LargeNumberColumn (verbose_name="Put bytes")
    PUT_MS = table_utils.LargeNumberColumn (verbose_name="Put seconds", divider=1000)


class JobsTable_HBaseDel (JobsTableBase, tables.Table):
    class Meta:
        attrs = {"class": "paleblue"}

    DELETE_CALLS = table_utils.LargeNumberColumn (verbose_name="Delete calls")
    DELETE_MS = table_utils.LargeNumberColumn (verbose_name="Delete sec", divider=1000)


class JobsTable_HBaseGet (JobsTableBase, tables.Table):
    class Meta:
        attrs = {"class": "paleblue"}

    GET_CALLS = table_utils.LargeNumberColumn (verbose_name="Get calls")
    GET_KVS = table_utils.LargeNumberColumn (verbose_name="Get KVs")
    GET_BYTES = table_utils.LargeNumberColumn (verbose_name="Get bytes")
    GET_MS = table_utils.LargeNumberColumn (verbose_name="Get sec", divider=1000)


class JobsTable_HBaseScan (JobsTableBase, tables.Table):
    class Meta:
        attrs = {"class": "paleblue"}

    SCAN_NEXT_MS = table_utils.LargeNumberColumn (verbose_name="Scan next sec", divider=1000)
    SCAN_NEXT_COUNT = table_utils.LargeNumberColumn (verbose_name="Scan next count")
    SCAN_ROWS = table_utils.LargeNumberColumn (verbose_name="Scan rows")
    SCAN_KVS = table_utils.LargeNumberColumn (verbose_name="Scan KVs")
    SCAN_BYTES = table_utils.LargeNumberColumn (verbose_name="Scan bytes")


class JobsTable_HBaseScanRS (JobsTableBase, tables.Table):
    class Meta:
        attrs = {"class": "paleblue"}

    SCAN_DISK_BYTES = table_utils.LargeNumberColumn (verbose_name="Scan HFile bytes")
    SCAN_READ_MS = table_utils.LargeNumberColumn (verbose_name="Scan HFile sec", divider=1000)


class JobDetailTable (tables.Table):
    class Meta:
        attrs = {"class": "paleblue"}
        orderable = False
    name = tables.Column ()
    value = tables.Column ()


class JobCountersTable (tables.Table):
    class Meta:
        attrs = {"class": "paleblue"}
        orderable = False

    group = tables.Column ()
    name = tables.Column ()
    value = table_utils.LargeNumberColumn ()
    origin_value = tables.Column ()
