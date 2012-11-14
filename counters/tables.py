import django_tables2 as tables

from counters.models import Pool

import datetime

class MillisecondColumn (tables.Column):
    """
    A column that renders milliseconds timedelta value into human-readable form
    """
    def render (self, value):
        return str (datetime.timedelta (seconds=long (value/1000)))


class PoolsResourcesTable (tables.Table):
    pool = tables.Column ()
    time = MillisecondColumn ()
    map_time = MillisecondColumn ()
    reduce_time = MillisecondColumn ()

    class Meta:
        attrs = {"class": "paleblue"}
