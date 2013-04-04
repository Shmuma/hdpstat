from django.shortcuts import render, redirect
from django.http import Http404

import models
import reports
from tables import HBaseTablesTable, DetailsTable, CFsTable
from hdpstat import table_utils

from django.utils import timezone
from django_tables2 import RequestConfig

import datetime


def dashboard_view (request):
    """
    Index view of hbase tables
    """
    # do not show tables with data older than that
    table_expire_days = 14
    now = timezone.now ()

    # get tables with data
    max_dt = None
    tables_data = []
    for table in models.Table.objects.all ().order_by ("name"):
        # get latests table sample
        data, s_dt = reports.get_table_sample (name=table.name, before=now)
        if now - s_dt < datetime.timedelta (days=table_expire_days):
            tables_data.append (data)
            if max_dt == None or max_dt < s_dt:
                max_dt = s_dt

    table = HBaseTablesTable (tables_data)   
    RequestConfig (request, paginate=False).configure (table)

    return render (request, "tables/dashboard.html", {'table': table, 'title': 'Tables overview',
                                                      'sample_date': str (max_dt)})

def table_detail_view (request, table, sample=None):
    if sample == None:
        s = models.TableSample.objects.filter (table__name=table).order_by ("-sample__date")[0]
    else:
        s = models.TableSample.objects.get (id=sample)

    if s.table.name != table:
        raise Http404

    data = [('Table name', table),
            ('Stat sampled', s.sample.date),
            ('Total size', table_utils.LargeNumberColumn.format (s.size)),
            ('Avg region size', table_utils.LargeNumberColumn.format (s.regionSizeAvg)),
            ('Regions count', s.regions),
            ('Oldest HFile date', s.oldestHFile),
            ('Oldest HFile age', reports.dt_minus_date (s.sample.date, s.oldestHFile)),
            ('Count HFiles', s.hfileCount),
            ('Average HFiles in region', "%.2f" % s.hfileCountAvg),
            ('Max HFiles in region', s.hfileCountMax)]

    data_table = DetailsTable ([{'name': n, 'value': v} for n, v in data])

    cf_table = CFsTable (reports.get_cf_data (s))

    RequestConfig (request, paginate=False).configure (cf_table)

    return render (request, "tables/table.html", {'table': data_table, 'cf_table': cf_table,
                                                  'name': table, 'sample': sample})


def cf_detail_view (request, table, cf, sample=None):
    if sample == None:
        s = models.CFSample.objects.filter (cf__table__name=table, cf__name=cf).order_by ("-sample__date")[0]
    else:
        s = models.CFSample.objects.get (id=sample)

    # sanity check
    if s.cf.name != cf or s.cf.table.name != table:
        raise Http404

    print s

    data = [('Table name', table),
            ('CF name', cf),
            ('Stat sampled', s.sample.date),
            ('Total size', table_utils.LargeNumberColumn.format (s.size)),
            ('Average size', table_utils.LargeNumberColumn.format (s.avgSize)),
            ('HFiles count', s.hfileCount),
            ('Average HFiles', "%.2f" % s.hfileCountAvg),
            ('Max HFiles', s.hfileCountMax)]
    data_table = DetailsTable ([{'name': n, 'value': v} for n, v in data])

    return render (request, "tables/cf.html", {'table': data_table, 'table_name': table,
                                               'cf_name': cf, 'sample': s.sample})
