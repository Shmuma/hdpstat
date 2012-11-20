import datetime

from django.utils import timezone
from django.http import HttpResponse
from django.shortcuts import render
from django_tables2 import RequestConfig, Table

from counters.models import Pool
from counters.reports import pools_resources_all_time, pools_resources_interval, jobs_history
from counters.tables import PoolsResourcesTable, make_jobs_table


def overview_pools_all_time (request):
    table = PoolsResourcesTable (pools_resources_all_time ())
    RequestConfig (request, paginate=False).configure (table)
    title = "All-time CPU time usage by pools"
    return render (request, 'counters/overview_resources.html', {'table': table, 'title': title})


def overview_pools_interval (request):
    back_days = int (request.GET.get ("days", 14))
    dt_to = datetime.datetime.now (timezone.get_current_timezone ())
    dt_from = dt_to - datetime.timedelta (days=back_days)

    data = pools_resources_interval (dt_from=dt_from, dt_to=dt_to)
    table = PoolsResourcesTable (data)

    RequestConfig (request, paginate=False).configure (table)
    title = "CPU time usage by pools %d days back" % back_days
    return render (request, 'counters/overview_resources.html', {'table': table, 'title': title})


def jobs_view (request):
    """
    Displays jobs list
    """
    # filters
    pool = request.GET.get ("pool")
    user = request.GET.get ("user")
    status = request.GET.get ("status")
    cgroup = request.GET.get ("cgroup", "Time")
    back_days = int (request.GET.get ("days", 14))

    dt_from = datetime.datetime.now (timezone.get_current_timezone ()) - datetime.timedelta (days=back_days)

    data = jobs_history (pool=pool, user=user, status=status, cgroup=cgroup, dt_from=dt_from)
    table = make_jobs_table (cgroup, data)

    RequestConfig (request, paginate=True).configure (table)
    title = "Jobs history"

    return render (request, "counters/overview_resources.html", {'table': table})
