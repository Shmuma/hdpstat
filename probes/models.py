from django.db import models


class HBase_Region (models.Model):
    full_name = models.CharField (max_length=10240)
    table = models.CharField (max_length=256)
    first_key = models.CharField (max_length=10240)


class HBase_Servers (models.Model):
    name = models.CharField (max_length=128, unique=True)
    time = models.FloatField ()
    probes = models.PositiveSmallIntegerField()


class HBase_RegionsTimes (models.Model):
    region = models.ForeignKey (HBase_Region)  
    time = models.FloatField ()
    probes = models.PositiveSmallIntegerField()


class HBase_LastProbes (models.Model):
    date = models.DateTimeField ()
    server = models.ForeignKey (HBase_Servers)
    region = models.ForeignKey (HBase_Region)
    time = models.PositiveIntegerField ()


class HBase_Alerts (models.Model):
    KINDS = (
        (0, 'GENERIC'),
        (1, 'SERVER'),
        (2, 'REGION'),
        )
    
    kind = models.PositiveIntegerField (choices=KINDS)
    date = models.DateTimeField ()
    text  = models.CharField (max_length=128)
    description = models.TextField (max_length=1024)
    
