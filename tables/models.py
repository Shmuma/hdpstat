from django.db import models


class Table (models.Model):
    name = models.CharField (max_length=64, unique=True)

    def __unicode__ (self):
        return self.name


class CF (models.Model):
    name = models.CharField (max_length=64)
    table = models.ForeignKey (Table)


class Sample (models.Model):
    date = models.DateTimeField (unique=True)

    def __unicode__ (self):
        return unicode (self.date)


class TableSample (models.Model):
    table = models.ForeignKey (Table)
    sample = models.ForeignKey (Sample)

    size = models.BigIntegerField ()
    regionSizeAvg = models.FloatField ()
    regions = models.PositiveIntegerField ()

    oldestHFile = models.DateField (null=True)
    hfileCountMax = models.PositiveIntegerField (null=True)
    hfileCountAvg = models.FloatField (null=True)
    hfileCount = models.PositiveIntegerField (null=True)

    def __unicode__ (self):
        return "table=%s, sampled=%s" % (self.table.name, str (self.sample.date))



class CFSample (models.Model):
    cf = models.ForeignKey (CF)
    sample = models.ForeignKey (Sample)

    size = models.BigIntegerField ()
    avgSize = models.FloatField ()
    hfileCountMax = models.PositiveIntegerField (null=True)
    hfileCountAvg = models.FloatField (null=True)
    hfileCount = models.PositiveIntegerField (null=True)

    def __unicode__ (self):
        return "cf=%s, sampled=%s" % (self.cf.name, str (self.sample.date))
