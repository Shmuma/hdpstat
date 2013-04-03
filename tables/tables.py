import django_tables2 as tables
from django_tables2.utils import A

from hdpstat import table_utils

class HBaseTablesTable (tables.Table):
    name = tables.Column ()
    size = table_utils.LargeNumberColumn ()
    regions = tables.Column ()
    avgreg = table_utils.LargeNumberColumn ()
    hfiles = tables.Column ()
    hfileAge = table_utils.TimedeltaColumn ()

    class Meta:
        attrs = {"class": "paleblue"}
    
