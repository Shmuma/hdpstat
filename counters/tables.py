import django_tables2 as tables

from counters.models import Pool


class PoolsResourcesTable (tables.Table):
    pool = tables.Column ()
    time = tables.Column ()


class PoolTable (tables.Table):
    class Meta:
        model = Pool
        attrs = {"class": "paleblue"}
