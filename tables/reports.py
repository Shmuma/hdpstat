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
        hfileAge = before - datetime.datetime.combine (s.oldestHFile, datetime.time ()).replace (tzinfo=utc)

    data = {'name': s.table.name, 'size': s.size, 'regions': s.regions,
            'avgreg': s.regionSizeAvg, 'hfiles': s.hfileCount, 'hfileAge': hfileAge}
    return data, s.sample.date
