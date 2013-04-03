from django.shortcuts import render, redirect

import models
import reports
from tables import HBaseTablesTable

from django.utils import timezone
import datetime


def dashboard_view (request):
    """
    Index view of hbase tables
    """
    # do not show tables with data older than that
    table_expire_days = 14
    now = timezone.now ()

    # get tables with data
    tables_data = []
    for table in models.Table.objects.all ().order_by ("name"):
        # get latests table sample
        data, s_dt = reports.get_table_sample (name=table.name, before=now)
        if now - s_dt < datetime.timedelta (days=table_expire_days):
            tables_data.append (data)

    table = HBaseTablesTable (tables_data)

    return render (request, "tables/dashboard.html", {'table': table})
