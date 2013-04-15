from django.shortcuts import render, redirect
from django.http import HttpResponse, Http404
from django.core.urlresolvers import reverse

import models
import reports
import charts
from tables import HBaseTablesTable, DetailsTable, CFsTable
from hdpstat import table_utils

from django.utils import timezone
from django_tables2 import RequestConfig

import os
import datetime
import subprocess

def dashboard_view (request, sample=None):
    """
    Index view of hbase tables
    """
    if sample == None:
        now = timezone.now ()
    else:
        now = models.Sample.objects.get (id=sample).date

    navigation = reports.get_navigations (now)

    # do not show tables with data older than that
    table_expire_days = 14

    # get tables with data
    max_dt = None
    tables_data = []
    total_data = {'size': 0, 'regions': 0, 'hfiles': 0}
    for table in models.Table.objects.all ().order_by ("name"):
        # get latests table sample
        data, s_dt = reports.get_table_sample (name=table.name, before=now)
        if data == None:
            continue
        if now - s_dt < datetime.timedelta (days=table_expire_days):
            tables_data.append (data)
            if max_dt == None or max_dt < s_dt:
                max_dt = s_dt
            for k in ['size', 'regions', 'hfiles']:
                total_data[k] = data[k] + total_data.get (k, 0)

    total = [('Total size', table_utils.LargeNumberColumn.format (total_data['size'])),
             ('Regions count', total_data['regions']),
             ('HFiles count', total_data['hfiles'])]

    total_table = DetailsTable ([{'name': n, 'value': v} for n, v in total])

    table = HBaseTablesTable (tables_data)   
    RequestConfig (request, paginate=False).configure (table)

    return render (request, "tables/dashboard.html", {'total_table': total_table, 'table': table,
                                                      'title': 'Tables overview',
                                                      'sample_date': max_dt,
                                                      'prev_day': navigation[0],
                                                      'next_day': navigation[1]})

def table_detail_view (request, table, sample=None):
    if sample == None:
        s = models.TableSample.objects.filter (table__name=table).order_by ("-sample__date")[0]
    else:
        s = models.TableSample.objects.get (id=sample)

    if s.table.name != table:
        raise Http404

    navigation = reports.get_table_navigations (s.sample.date, table)
    if s.hfileCountAvg:
        avg_hfiles = "%.2f" % s.hfileCountAvg
    else:
        avg_hfiles = None

    data = [('Table name', table),
            ('Stat sampled', s.sample.date),
            ('Total size', table_utils.LargeNumberColumn.format (s.size)),
            ('Avg region size', table_utils.LargeNumberColumn.format (s.regionSizeAvg)),
            ('Regions count', s.regions),
            ('Oldest HFile date', s.oldestHFile),
            ('Oldest HFile age', reports.dt_minus_date (s.sample.date, s.oldestHFile)),
            ('Count HFiles', s.hfileCount),
            ('Average HFiles in region', avg_hfiles),
            ('Max HFiles in region', s.hfileCountMax)]

    data_table = DetailsTable ([{'name': n, 'value': v} for n, v in data])

    cf_table = CFsTable (reports.get_cf_data (s))

    RequestConfig (request, paginate=False).configure (cf_table)

    return render (request, "tables/table.html", {'table': data_table, 'cf_table': cf_table,
                                                  'name': table, 'sample': sample,
                                                  'sample_date': s.sample.date,
                                                  'prev_day': navigation[0],
                                                  'next_day': navigation[1]})


def cf_detail_view (request, table, cf, sample=None):
    if sample == None:
        s = models.CFSample.objects.filter (cf__table__name=table, cf__name=cf).order_by ("-sample__date")[0]
    else:
        s = models.CFSample.objects.get (id=sample)

    # sanity check
    if s.cf.name != cf or s.cf.table.name != table:
        raise Http404

    data = [('Table name', table),
            ('CF name', cf),
            ('Stat sampled', s.sample.date),
            ('Total size', table_utils.LargeNumberColumn.format (s.size)),
            ('Average size', table_utils.LargeNumberColumn.format (s.avgSize)),
            ('HFiles count', s.hfileCount),
            ('Average HFiles', "%.2f" % s.hfileCountAvg),
            ('Max HFiles', s.hfileCountMax)]
    data_table = DetailsTable ([{'name': n, 'value': v} for n, v in data])

    navigation = reports.get_cf_navigations (s.sample.date, table, cf)

    return render (request, "tables/cf.html", {'table': data_table, 'table_name': table,
                                               'cf_name': cf, 'sample': s.sample,
                                               'sample_date': s.sample.date,
                                               'prev_day': navigation[0],
                                               'next_day': navigation[1]})

def chart_tables (request, kind):
    """
    Misc charts for tables dashboard
    """
    now = timezone.now ()

    kinds = {'size':          ("Size of tables for %s", "Size", "B", True, None),
             'regionSizeAvg': ("Average size of regions for %s", "Size", "B", True, None),
             'regions':       ("Regions count for %s", "Regions", "", True, None),
             'hfileCount':    ("HFiles count for %s", "HFiles", "", True, None),
             'oldestHFile':   ("Max age of HFiles for %s", "Days", "", False,
                               lambda oldest: reports.dt_minus_date (now, oldest).total_seconds () / 86400.0)}

    if not kind in kinds:
        raise Http404

    title, yaxis, suff, aggr, filter = kinds[kind]

    back_days, period_name = reports.get_chart_period (request.GET.get ('period', '2weeks'))

    keys, data_table = reports.get_tables_chart_data (back_days, kind, filter=filter)
    chart_data = charts.format_chart_data (keys, data_table, aggregate=aggr)
    mult, suffix = charts.data_multiplier (data_table)
    
    pls_file = charts.generate_pls (items=keys, title=title % period_name,
                                    yaxis=yaxis, mult=mult, suffix=suffix+suff, area=True)
    image = charts.generate_chart (pls_file, chart_data)

    if image == None:
        raise Http404

    resp = HttpResponse (content_type='image/png')
    resp.write (image)
    os.unlink (pls_file)
    return resp


def chart_details (request, view, kind):
    """
    Display bunch of charts for tables
    """
    return render (request, "tables/chart_details.html", { 'url': reverse (view, args=(kind,)) })
