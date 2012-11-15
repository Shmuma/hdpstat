import django_tables2 as tables

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


    def __init__ (self, multiplier = 1.0, **extra):
        super (LargeNumberColumn, self).__init__ (**extra)
        self.multiplier = multiplier


    def render (self, value):
        value *= self.multiplier

        prev_power = 0
        prev_suffix = ''
        for power in sorted (self.power_to_suffix.keys ()):
            if value < pow (10, power):
                return "%.2f%s" % (value / pow (10, prev_power), prev_suffix)
            prev_power = power
            prev_suffix = self.power_to_suffix[power]


class PoolsResourcesTable (tables.Table):
    pool = tables.Column ()
    time = LargeNumberColumn (0.001)
    map_time = LargeNumberColumn (0.001)
    reduce_time = LargeNumberColumn (0.001)

    class Meta:
        attrs = {"class": "paleblue"}



class JobsTable (tables.Table):
    class Meta:
        attrs = {"class": "paleblue"}

    jobid = tables.Column ()
    status = tables.Column ()
    pool = tables.Column ()
    user = tables.Column ()

    submitted = tables.DateTimeColumn ()
    duration = TimedeltaColumn ()

    HDFS_BYTES_WRITTEN = LargeNumberColumn ()
    HDFS_BYTES_READ = LargeNumberColumn ()
