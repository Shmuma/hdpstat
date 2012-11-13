import django_tables2 as tables

from counters.models import Pool

import datetime

class MilisecondColumn (tables.Column):
    """
    A column that renders miliseconds timedelta value into human-readable form
    """
    def render (self, value):
        return str (datetime.timedelta (seconds=long (value/1000)))


class PoolsResourcesTable (tables.Table):
    pool = tables.Column ()
    time = MilisecondColumn ()

    class Meta:
        attrs = {"class": "paleblue"}
