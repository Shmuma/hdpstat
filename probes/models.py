from django.db import models

from lib.sliding_averager import SlidingWindowAverager, SlidingWindowAveragerState


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


class TimeProbesHolder (models.Model):
    """
    Abstract common parent for ServerTime and RegionTime models
    """
    time = models.FloatField (default=0.0)
    probes = models.PositiveSmallIntegerField(default=0)
    half_time = models.FloatField (default=0.0)
    half_probes = models.PositiveSmallIntegerField(default=0)

    def get_avg_state (self):
        return SlidingWindowAveragerState(self.time*self.probes, self.probes, self.half_time*self.half_probes, self.half_probes)

    def set_avg_state (self, state):
        if state.full_count > 0:
            self.time = state.full_sum / state.full_count
            self.probes = state.full_count
        else:
            self.time = 0.0
            self.probes = 0
        if state.half_count > 0:
            self.half_time = state.half_sum / state.half_count
            self.half_probes = state.half_count
        else:
            self.half_time = 0.0
            self.half_count = 0
        
    averager_state = property(get_avg_state, set_avg_state)
    
    class Meta:
        abstract = True


class HBase_ServerTime (TimeProbesHolder):
    server = models.ForeignKey(HBase_Server, unique=True)


class HBase_RegionTime (TimeProbesHolder):
    region = models.ForeignKey (HBase_Region, unique=True)  


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
    date = models.DateTimeField (auto_now_add=True)
    objname = models.CharField (max_length=128)
    text  = models.CharField (max_length=128)
    description = models.TextField (max_length=1024)
    
