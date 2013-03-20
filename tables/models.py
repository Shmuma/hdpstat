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
        return self.date


class TableSample (models.Model):
    table = models.ForeignKey (Table)
    sample = models.ForeignKey (Sample)

    size = models.BigIntegerField ()
    regionSizeAvg = models.FloatField ()
    regions = models.PositiveIntegerField ()
    splits = models.PositiveIntegerField (null=True)

    oldestHFile = models.DateTimeField (null=True)
    hfileCountMax = models.PositiveIntegerField (null=True)
    hfileCountAvg = models.FloatField (null=True)
    hfileCount = models.PositiveIntegerField (null=True)


class CFSample (models.Model):
    cf = models.ForeignKey (CF)
    sample = models.ForeignKey (Sample)

    size = models.BigIntegerField ()
    hfileCountMax = models.PositiveIntegerField (null=True)
    hfileCountAvg = models.FloatField (null=True)
    hfileCount = models.PositiveIntegerField (null=True)
    
