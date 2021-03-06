import models

import datetime
from django.utils.timezone import utc, now
from django.db import connection

def get_table_sample (name, before):
    """
    Return hash with nearest table sample before specified datetime
    """
    samples = models.TableSample.objects.filter (table__name=name, sample__date__lt=before).order_by ('-sample__date')
    if len (samples) == 0:
        return None, None
    s = samples[0]
    if s.oldestHFile == None:
        hfileAge = None
    else:
        hfileAge = dt_minus_date (before, s.oldestHFile)

    data = {'name': s.table.name, 'size': s.size, 'regions': s.regions,
            'avgreg': s.regionSizeAvg, 'hfiles': s.hfileCount, 'hfileAge': hfileAge,
            'sample_id': s.pk}
    return data, s.sample.date



def dt_minus_date (dt, date):
    """
    Returns timedelta between datetitme and date
    """
    if date == None:
        return None
    return dt - datetime.datetime.combine (date, datetime.time ()).replace (tzinfo=utc)


def get_cf_data (tsample):
    res = []
    for cfs in models.CFSample.objects.filter (sample=tsample.sample, cf__table=tsample.table).order_by ('cf__name'):
        res.append ({'name': cfs.cf.name,
                     'size': cfs.size,
                     'avgSize': cfs.avgSize,
                     'hfiles': cfs.hfileCount,
                     'hfilesAvg': cfs.hfileCountAvg,
                     'hfilesMax': cfs.hfileCountMax,
                     'cf': cfs.cf.name,
                     'table': tsample.table.name,
                     'sample_id': cfs.pk})
    return res


def get_navigations (dt_now):
    """
    Return tuple of 2 samples: (prev_day, next_day)
    If some not found, return None instead this entry
    """
    def get_sample_before (dt):
        samples = models.Sample.objects.filter (date__lt=dt).order_by ("-date")
        if len (samples) > 0:
            return samples[0]
        else:
            return None

    def get_sample_after (dt):
        samples = models.Sample.objects.filter (date__gt=dt).order_by ("date")
        if len (samples) > 0:
            return samples[0]
        else:
            return None

    prev_day = get_sample_before (dt_now - datetime.timedelta (days=1))
    next_day = get_sample_after (dt_now + datetime.timedelta (days=1))
    return (prev_day, next_day)



def get_table_navigations (dt_now, table):
    """
    Return tuple of 2 samples: (prev_day, next_day)
    If some not found, return None instead this entry
    """
    def get_sample_before (dt):
        samples = models.TableSample.objects.filter (sample__date__lt=dt, table__name=table).order_by ("-sample__date")
        if len (samples) > 0:
            return samples[0]
        else:
            return None

    def get_sample_after (dt):
        samples = models.TableSample.objects.filter (sample__date__gt=dt, table__name=table).order_by ("sample__date")
        if len (samples) > 0:
            return samples[0]
        else:
            return None

    prev_day = get_sample_before (dt_now - datetime.timedelta (days=1))
    next_day = get_sample_after (dt_now + datetime.timedelta (days=1))
    return (prev_day, next_day)


def get_cf_navigations (dt_now, table, cf):
    """
    Return tuple of 2 samples: (prev_day, next_day)
    If some not found, return None instead this entry
    """
    def get_sample_before (dt):
        samples = models.CFSample.objects.filter (sample__date__lt=dt, cf__name=cf,
                                                  cf__table__name=table).order_by ("-sample__date")
        if len (samples) > 0:
            return samples[0]
        else:
            return None

    def get_sample_after (dt):
        samples = models.CFSample.objects.filter (sample__date__gt=dt, cf__name=cf,
                                                  cf__table__name=table).order_by ("sample__date")
        if len (samples) > 0:
            return samples[0]
        else:
            return None

    prev_day = get_sample_before (dt_now - datetime.timedelta (days=1))
    next_day = get_sample_after (dt_now + datetime.timedelta (days=1))
    return (prev_day, next_day)


def get_tables_chart_data (back_days, table_sample_field, filter=None, table=None):
    """
    Builds list of tables and data for tables overview charts.
    Accessor is applied to TableSample object to obtain numeric value
    """
    if back_days != None:
        dt_limit = now () - datetime.timedelta (days=back_days)
    else:
        dt_limit = datetime.datetime (1970, 1, 1, tzinfo=utc)

    data_table = {}
    keys = []

    for table_obj in models.Table.objects.order_by ("name"):
        k = table_obj.name
        if table != None and table != k:
            continue
        count = 0

        sql = """select s.date, ts.""" + table_sample_field + """ from tables_tablesample ts, 
                 tables_sample s, tables_table t
                 where ts.table_id = t.id and ts.sample_id = s.id and s.date >= %(date_limit)s and 
                 t.name = %(table)s"""

        args = { 'date_limit': dt_limit,  'table': k }

        cur = connection.cursor ()
        cur.execute (sql, args)
        for date, value in cur.fetchall ():
            if value == None:
                continue
            count += 1
            d = date.replace (minute=0, second=0, microsecond=0)

            if not d in data_table:
                data_table[d] = {}
            if filter != None:
                value = filter (value)
            data_table[d][k] = value

        if count > 0:
            keys.append (k)

    return keys, data_table


def get_cfs_chart_data (back_days, cf_sample_field, table, filter=None, cf=None):
    """
    Builds list of tables and data for tables overview charts.
    Accessor is applied to TableSample object to obtain numeric value
    """
    if back_days != None:
        dt_limit = now () - datetime.timedelta (days=back_days)
    else:
        dt_limit = datetime.datetime (1970, 1, 1, tzinfo=utc)

    data_table = {}
    keys = []

    for cf_obj in models.CF.objects.filter (table__name=table).order_by ('name'):
        k = cf_obj.name
        if cf != None and k != cf:
            continue
        count = 0

        sql = """select s.date, cs.""" + cf_sample_field + """ from tables_cfsample cs, 
                 tables_sample s, tables_table t, tables_cf cf
                 where cf.table_id = t.id and cs.sample_id = s.id and cf.id = cs.cf_id and 
                 s.date >= %(date_limit)s and cs.cf_id = %(cf_id)s"""

        args = { 'date_limit': dt_limit,  'cf_id': cf_obj.id }

        cur = connection.cursor ()
        cur.execute (sql, args)
        for date, value in cur.fetchall ():
            if value == None:
                continue
            count += 1
            d = date.replace (minute=0, second=0, microsecond=0)

            if not d in data_table:
                data_table[d] = {}
            if filter != None:
                value = filter (value)
            data_table[d][k] = value

        if count > 0:
            keys.append (k)

    return keys, data_table



def get_chart_period (period):
    """
    By named period, return pair of days number and human-readable period name
    """
    data = {'2weeks': (14, "two weeks"),
            'month': (30, "a month"),
            '3months': (90, "three months"),
            'year': (365, "a year")}

    return data.get (period, (None, "all time"))
