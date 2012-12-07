import datetime

from django.utils import timezone
from django.http import HttpResponse
from django.shortcuts import render
from django_tables2 import RequestConfig, Table

from counters.models import Pool
from counters.reports import pools_resources_all_time, pools_resources_interval, jobs_history, job_detail_data, job_counters_data
from counters.tables import PoolsResourcesTable, make_jobs_table, JobDetailTable, JobCountersTable

from counters import filters


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
    pools = request.GET.getlist ("pool")
    users = request.GET.getlist ("user")
    statuses = request.GET.getlist ("status")
    cgroup = request.GET.get ("cgroup", "Time")
    back_days = int (request.GET.get ("days", 1))
    job_name = request.GET.get ("job_name", "")

    filter_form = filters.FilterForm (request.GET)

    dt_from = datetime.datetime.now (timezone.get_current_timezone ()) - datetime.timedelta (days=back_days)

    data = jobs_history (pools=pools, users=users, statuses=statuses, cgroup=cgroup, dt_from=dt_from, job_name=job_name)
    table = make_jobs_table (cgroup, data)

    RequestConfig (request, paginate=True).configure (table)
    title = "Jobs history"

    return render (request, "counters/jobs.html", {'table': table, 'filter_form': filter_form})


def job_detail_view (request, jobid):
    data = job_detail_data (jobid)
    table = JobDetailTable (data)

    counters_data = job_counters_data (jobid)
    counters_table = JobCountersTable (counters_data)
    return render (request, "counters/job_detail.html", {'jobid': jobid, 'table': table, 'counters_table': counters_table})


def dashboard_view (request):
    return render (request, "counters/dashboard.html")
