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
    back_days = int (request.GET.get ("days", 14))
    dt_to = datetime.datetime.now (timezone.get_current_timezone ())
    dt_from = dt_to - datetime.timedelta (days=back_days)
    data = pools_resources_interval (dt_from=dt_from, dt_to=dt_to)
    table = PoolsResourcesTable (data)

    RequestConfig (request, paginate=False).configure (table)
    return render (request, 'counters/overview_resources.html', {'table': table, 'title': "CPU time usage by pools %d days back" % back_days})
