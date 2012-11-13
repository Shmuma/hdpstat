import datetime

from django.utils import timezone
from django.http import HttpResponse
from django.shortcuts import render
from django_tables2 import RequestConfig, Table

from counters.models import Pool
from counters.reports import pools_resources_all_time, pools_resources_interval
from counters.tables import PoolsResourcesTable


def overview_pools_all_time (request):
    table = PoolsResourcesTable (pools_resources_all_time ())
    RequestConfig (request, paginate=False).configure (table)
    return render (request, 'counters/overview_resources.html', {'table': table, 'title': "All-time CPU time usage by pools"})


def overview_pools_interval (request):
    # three days back
    dt_to = datetime.datetime (year=2012, month=11, day=1, tzinfo=timezone.get_current_timezone ())
    dt_from = dt_to - datetime.timedelta (days=3)
    report = pools_resources_interval (dt_from=dt_from, dt_to=dt_to)
    res = "from = %s, to = %s</br></br>" % (dt_from, dt_to)
    for r in report.keys ():
        res += "%s =></br>" % r
        for ti in report[r]:
            res += "%s, started=%s, finished=%s</br>" % (ti, ti.started, ti.finished)
    return HttpResponse (res)
