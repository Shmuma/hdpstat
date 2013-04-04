import models

import datetime
from django.utils.timezone import utc

def get_table_sample (name, before):
    """
    Return hash with nearest table sample before specified datetime
    """
    s = models.TableSample.objects.filter (table__name=name, sample__date__lt=before).order_by ('-sample__date')[0]
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
