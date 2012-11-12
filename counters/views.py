from django.http import HttpResponse
from django.shortcuts import render
from django_tables2 import RequestConfig, Table

from counters.models import Pool
from counters.reports import pools_resources_all_time
from counters.tables import PoolsResourcesTable, PoolTable


def test (request):
    return HttpResponse ("Hello, world!")


def overview_pools_all_time (request):
    table = PoolsResourcesTable (pools_resources_all_time ())
    RequestConfig (request).configure (table)
    return render (request, 'counters/overview_resources.html', {'table': table})
