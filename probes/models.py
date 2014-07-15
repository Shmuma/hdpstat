from django.db import models


class HBase_Region (models.Model):
    name = models.CharField (max_length=10240)

    def _get_table (self):
        return self._get_regfield(0)

    def _get_firstkey (self):
        return self._get_regfield(1)
    
    def _get_reghash (self):
        v = self._get_regfield(2).split(".")
        if len(v) >= 2:
            return v[1]

    def _get_regfield (self, index):
        v = self.name.split(",")
        if index >= len(v):
            return None
        return v[index]

    table = property (_get_table)
    firstkey = property (_get_firstkey)
    reghash = property (_get_reghash)


    
class HBase_Server (models.Model):
    name = models.CharField (max_length=128, unique=True)


class HBase_ServerTime (models.Model):
    server = models.ForeignKey(HBase_Server)
    time = models.FloatField ()
    probes = models.PositiveSmallIntegerField()


class HBase_RegionTime (models.Model):
    region = models.ForeignKey (HBase_Region)  
    time = models.FloatField ()
    probes = models.PositiveSmallIntegerField()


class HBase_LastProbe (models.Model):
    date = models.DateTimeField (auto_now_add=True)
    server = models.ForeignKey (HBase_Server)
    region = models.ForeignKey (HBase_Region)
    time = models.PositiveIntegerField ()


class HBase_Alert (models.Model):
    KINDS = (
        (0, 'GENERIC'),
        (1, 'SERVER'),
        (2, 'REGION'),
        )
    
    kind = models.PositiveIntegerField (choices=KINDS)
    date = models.DateTimeField ()
    text  = models.CharField (max_length=128)
    description = models.TextField (max_length=1024)
    
