import django_tables2 as tables
from django_tables2.utils import A

from hdpstat import table_utils


class HBaseTablesTable (tables.Table):
    name = tables.LinkColumn ('table_detail', kwargs={'table': A('name'), 'sample': A('sample_id')})
    size = table_utils.LargeNumberColumn ()
    regions = tables.Column ()
    avgreg = table_utils.LargeNumberColumn ()
    hfiles = tables.Column ()
    hfileAge = table_utils.TimedeltaColumn ()

    class Meta:
        attrs = {"class": "paleblue"}


class TableDetailsTable (tables.Table):
    name = tables.Column ()
    value = tables.Column ()

    class Meta:
        attrs = {"class": "paleblue"}
        orderable = False


class CFsTable (tables.Table):
    name = tables.Column ()
    size = table_utils.LargeNumberColumn ()
    avgSize = table_utils.LargeNumberColumn ()
    hfiles = tables.Column ()
    hfilesAvg = table_utils.LargeNumberColumn ()
    hfilesMax = tables.Column ()

    class Meta:
        attrs = {"class": "paleblue"}
    
